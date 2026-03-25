#![allow(unused_crate_dependencies)]

use common::garage::GarageInstance;
use derive_more::Display;
use jiff::{Span, Unit, Zoned};
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng, rng};
use reqwest::Client;
use rootcause::{Report, bail};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::ops::{Range, Sub};
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;
use server::config::{AddressingStyle, Config, S3Secret, Upstream, UpstreamId};
use server::data::{S3Object, S3ObjectId};
use server::db::Database;
use server::migrate::{execute_pending_migrations, get_pending_migrations};
use server::s3_client::client::{ObjectInfo, S3Client};

mod common;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
enum Tier {
    Hot,
    Warm,
    Cold,
}

impl Tier {
    fn order(&self) -> usize {
        match self {
            Self::Hot => 1,
            Self::Warm => 2,
            Self::Cold => 3,
        }
    }

    fn max_age(&self) -> Option<Span> {
        match self {
            Self::Hot => Some(Span::new().hours(2)),
            Self::Warm => Some(Span::new().hours(5)),
            Self::Cold => None,
        }
    }

    fn age_as_hour_range(&self) -> Range<f64> {
        match self {
            Self::Hot => 0f64..2f64,
            Self::Warm => 2.1f64..5.0f64,
            Self::Cold => 5.1f64..12.0f64,
        }
    }

    fn all() -> [Self; 3] {
        [Self::Hot, Self::Warm, Self::Cold]
    }
}

impl TryFrom<&str> for Tier {
    type Error = Report;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Hot" => Ok(Self::Hot),
            "Warm" => Ok(Self::Warm),
            "Cold" => Ok(Self::Cold),
            _ => bail!("Unknown tier: '{value}'"),
        }
    }
}

static GARAGE_HOT: OnceCell<GarageInstance> = OnceCell::const_new();
static GARAGE_WARM: OnceCell<GarageInstance> = OnceCell::const_new();
static GARAGE_COLD: OnceCell<GarageInstance> = OnceCell::const_new();

#[derive(Debug, Clone)]
struct SeededObject {
    id: S3ObjectId,
    expected: Tier,
    payload: Vec<u8>,
}

struct TierBackend {
    upstream: Upstream,
    client: S3Client,
}

#[ctor::dtor]
fn shutdown_garages() {
    [
        GARAGE_HOT.get().and_then(GarageInstance::take_container),
        GARAGE_WARM.get().and_then(GarageInstance::take_container),
        GARAGE_COLD.get().and_then(GarageInstance::take_container),
    ]
    .into_iter()
    .flatten()
    .for_each(GarageInstance::drop_in_new_runtime);
}

async fn garage_for_tier(tier: Tier) -> Result<&'static GarageInstance, Report> {
    match tier {
        Tier::Hot => GARAGE_HOT.get_or_try_init(GarageInstance::start).await,
        Tier::Warm => GARAGE_WARM.get_or_try_init(GarageInstance::start).await,
        Tier::Cold => GARAGE_COLD.get_or_try_init(GarageInstance::start).await,
    }
}

async fn setup_tier_backend(tier: Tier) -> Result<TierBackend, Report> {
    let garage = garage_for_tier(tier).await?;

    let bucket = "test";
    let bucket_id = garage.create_bucket(bucket).await?;
    let (key_id, secret) = garage.create_key(&format!("e2e-{}", tier)).await?;
    garage.allow_key_on_bucket(&bucket_id, &key_id).await?;

    let client = S3Client::new(
        Client::new(),
        garage.s3_endpoint().clone(),
        garage.region(),
        &key_id,
        &secret,
        AddressingStyle::Path,
    );

    Ok(TierBackend {
        upstream: Upstream {
            name: UpstreamId(tier.to_string()),
            order: tier.order(),
            base_url: garage.s3_endpoint().clone(),
            addressing_style: AddressingStyle::Path,
            max_age: tier.max_age(),
            s3_access_key: key_id,
            s3_secret: S3Secret(secret),
            region: garage.region().to_owned(),
        },
        client,
    })
}

#[tokio::test]
async fn get_pending_migrations_randomized_ages_match_expected_targets() -> Result<(), Report> {
    let db = Database::in_memory().await?;

    let seed = rng().next_u64();
    println!("seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);
    let now = Zoned::now();

    let mut expected = HashSet::new();
    let mut obj_map = HashMap::new();
    for source_tier in Tier::all() {
        let source_id = UpstreamId(source_tier.to_string());
        for index in 0..120 {
            let tier = Tier::all()
                .choose(&mut rng)
                .copied()
                .expect("tier choices must not be empty");
            let object = random_object_for_tier(&mut rng, &now, source_id.clone(), tier, index);
            obj_map.insert(object.id.clone(), object.clone());
            db.record_creation(&object).await?;

            if source_tier != tier {
                expected.insert((source_id.clone(), UpstreamId(tier.to_string()), object.id));
            }
        }
    }

    let pending = get_pending_migrations(&test_config()?, &db, now.clone()).await?;
    let got = pending
        .into_iter()
        .map(|pending| {
            (
                pending.source_upstream,
                pending.target_upstream,
                pending.object,
            )
        })
        .collect::<HashSet<_>>();

    println!("GOT");
    print_migrations(&now, &obj_map, &got)?;
    println!("EXPECTED");
    print_migrations(&now, &obj_map, &expected)?;

    assert_eq!(got, expected);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn e2e_executes_expected_migrations_across_three_upstreams() -> Result<(), Report> {
    const TEST_OBJECT_COUNT: usize = 42;
    let hot = setup_tier_backend(Tier::Hot).await?;
    let warm = setup_tier_backend(Tier::Warm).await?;
    let cold = setup_tier_backend(Tier::Cold).await?;

    let mut upstreams = HashMap::new();
    upstreams.insert(hot.upstream.name.clone(), hot.upstream.clone());
    upstreams.insert(warm.upstream.name.clone(), warm.upstream.clone());
    upstreams.insert(cold.upstream.name.clone(), cold.upstream.clone());
    let config = Config::new(
        "127.0.0.1:0".to_owned(),
        None,
        Path::new("/tmp/").to_path_buf(),
        upstreams,
    )?;

    let db = Database::in_memory().await?;
    let seed = rng().next_u64();
    println!("seed: {seed}");
    let mut rng = StdRng::seed_from_u64(seed);
    let now = Zoned::now();

    let mut expected_pending = HashSet::new();
    let mut seeded = Vec::new();

    for source in Tier::all() {
        let source_upstream = match source {
            Tier::Hot => &hot.upstream.name,
            Tier::Warm => &warm.upstream.name,
            Tier::Cold => &cold.upstream.name,
        };
        let source_client = match source {
            Tier::Hot => &hot.client,
            Tier::Warm => &warm.client,
            Tier::Cold => &cold.client,
        };

        for index in 0..TEST_OBJECT_COUNT {
            let expected_tier = Tier::all()
                .choose(&mut rng)
                .copied()
                .expect("tier choices must not be empty");
            let payload_len = rng.random_range(20..120);
            let payload = (0..payload_len)
                .map(|_| rng.random_range(0..=255) as u8)
                .collect::<Vec<_>>();

            let object = random_object_for_tier(
                &mut rng,
                &now,
                source_upstream.clone(),
                expected_tier,
                index,
            );

            source_client
                .put_file(
                    &object.id,
                    Cursor::new(payload.clone()),
                    payload.len() as u64,
                )
                .await?;
            db.record_creation(&object).await?;

            seeded.push(SeededObject {
                id: object.id.clone(),
                expected: expected_tier,
                payload,
            });

            if source != expected_tier {
                expected_pending.insert((
                    source_upstream.clone(),
                    UpstreamId(expected_tier.to_string()),
                    object.id,
                ));
            }
        }
    }

    let pending = get_pending_migrations(&config, &db, now.clone()).await?;
    let pending_set = pending
        .iter()
        .map(|it| {
            (
                it.source_upstream.clone(),
                it.target_upstream.clone(),
                it.object.clone(),
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(pending_set, expected_pending, "pending migrations mismatch");

    db.add_all_pending(&pending).await?;
    let errors = execute_pending_migrations(pending, &config, &db).await?;
    assert!(errors.is_empty(), "migration execution failed: {errors:?}");
    db.delete_finished_pending().await?;

    let tier_to_keys = [
        (
            Tier::Hot,
            keys_in_bucket(hot.client.list_objects("test").await?),
        ),
        (
            Tier::Warm,
            keys_in_bucket(warm.client.list_objects("test").await?),
        ),
        (
            Tier::Cold,
            keys_in_bucket(cold.client.list_objects("test").await?),
        ),
    ]
    .into_iter()
    .collect::<HashMap<Tier, HashSet<String>>>();

    for object in &seeded {
        let expected_upstream = UpstreamId(object.expected.to_string());
        assert_eq!(
            db.get_upstream(&object.id).await?,
            Some(expected_upstream),
            "db assignment mismatch for {}",
            object.id
        );

        for tier in Tier::all() {
            let keys = tier_to_keys.get(&tier).unwrap();
            if tier == object.expected {
                assert!(keys.contains(&object.id.key));
            } else {
                assert!(!keys.contains(&object.id.key));
            }
        }

        let owner_client = match object.expected {
            Tier::Hot => &hot.client,
            Tier::Warm => &warm.client,
            Tier::Cold => &cold.client,
        };
        let actual_payload = read_object(owner_client, &object.id).await?;
        assert_eq!(actual_payload, object.payload);
    }

    let left_pending = db.get_pending_with_state(None).await?;
    assert!(left_pending.is_empty());

    db.close().await?;
    Ok(())
}

fn keys_in_bucket(objects: Vec<ObjectInfo>) -> HashSet<String> {
    objects.into_iter().map(|it| it.key).collect()
}

async fn read_object(client: &S3Client, id: &S3ObjectId) -> Result<Vec<u8>, Report> {
    let (_, mut stream) = client.get_file(id).await?;
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await?;
    Ok(data)
}

fn random_object_for_tier(
    rng: &mut StdRng,
    now: &Zoned,
    source: UpstreamId,
    tier: Tier,
    index: usize,
) -> S3Object {
    let age_hours = rng.random_range(tier.age_as_hour_range());
    let age_seconds = (age_hours * 3600.0) as i64;

    S3Object {
        id: S3ObjectId {
            bucket: "test".to_owned(),
            key: format!("{}-{}-{index}", source, random_key(rng)),
        },
        assigned_upstream: source,
        last_modified: (now - Span::new().seconds(age_seconds)).timestamp(),
    }
}

fn random_key(rng: &mut StdRng) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..16)
        .map(|_| {
            let idx = rng.random_range(0..ALPHABET.len());
            ALPHABET[idx] as char
        })
        .collect()
}

fn test_config() -> Result<Config, Report> {
    let new_upstream = |id: UpstreamId, order: usize, max_age: Option<Span>| {
        Ok::<Upstream, Report>(Upstream {
            name: id,
            order,
            base_url: format!("http://127.0.0.1:900{order}").parse()?,
            addressing_style: AddressingStyle::Path,
            max_age,
            s3_access_key: "test".to_owned(),
            s3_secret: S3Secret("test".to_owned()),
            region: "test".to_owned(),
        })
    };

    let mut upstreams = HashMap::new();
    for (index, tier) in Tier::all().iter().enumerate() {
        let id = UpstreamId(tier.to_string());
        upstreams.insert(id.clone(), new_upstream(id, index + 1, tier.max_age())?);
    }

    Config::new(
        "127.0.0.1:0".to_owned(),
        None,
        Path::new("/tmp/").to_path_buf(),
        upstreams,
    )
}

fn print_migrations(
    now: &Zoned,
    obj_map: &HashMap<S3ObjectId, S3Object>,
    migrations: &HashSet<(UpstreamId, UpstreamId, S3ObjectId)>,
) -> Result<(), Report> {
    for (source, target, obj) in migrations {
        println!(
            "{obj:>40}  |  {source:<5} -> {target:<5} {:>5.2} | {} / {}",
            now.timestamp()
                .sub(obj_map.get(obj).unwrap().last_modified)
                .total(Unit::Hour)?,
            Tier::try_from(source.0.as_str())?
                .max_age()
                .map(|x| format!("{}h", x.get_hours()))
                .unwrap_or("None".to_string()),
            Tier::try_from(target.0.as_str())?
                .max_age()
                .map(|x| format!("{}h", x.get_hours()))
                .unwrap_or("None".to_string()),
        );
    }

    Ok(())
}
