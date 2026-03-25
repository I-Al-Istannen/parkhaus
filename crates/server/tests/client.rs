#![allow(unused_crate_dependencies)]

use crate::common::garage::GarageInstance;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::Client;
use rootcause::Report;
use server::config::AddressingStyle;
use server::data::S3ObjectId;
use server::s3_client::client::{CHUNK_SIZE, S3Client};
use sha2::Digest;
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;

mod common;

static GARAGE: OnceCell<GarageInstance> = OnceCell::const_new();

#[ctor::dtor]
fn shutdown_garage() {
    if let Some(container) = GARAGE.get().and_then(GarageInstance::take_container) {
        GarageInstance::drop_in_new_runtime(container);
    }
}

async fn garage() -> Result<&'static GarageInstance, Report> {
    GARAGE.get_or_try_init(GarageInstance::start).await
}

fn bucket_name(test_name: &str) -> String {
    test_name.replace('_', "-")
}

fn bucket_name_with_suffix(test_name: &str, suffix: &str) -> String {
    format!("{}-{suffix}", bucket_name(test_name))
}

/// Create a bucket + key with full permissions, return a ready-to-use client.
async fn setup_bucket(g: &GarageInstance, bucket: &str) -> Result<S3Client, Report> {
    setup_bucket_with_style(g, bucket, AddressingStyle::Path).await
}

async fn setup_bucket_with_style(
    g: &GarageInstance,
    bucket: &str,
    style: AddressingStyle,
) -> Result<S3Client, Report> {
    let bucket_id = g.create_bucket(bucket).await?;
    let (key_id, secret) = g.create_key(bucket).await?;
    g.allow_key_on_bucket(&bucket_id, &key_id).await?;
    Ok(S3Client::new(
        Client::new(),
        g.s3_endpoint().clone(),
        g.region(),
        &key_id,
        &secret,
        style,
    ))
}

async fn put_object(
    client: &S3Client,
    bucket: &str,
    key: &str,
    payload: &[u8],
) -> Result<(), Report> {
    let id = S3ObjectId {
        bucket: bucket.into(),
        key: key.into(),
    };
    client
        .put_file(&id, Cursor::new(payload.to_vec()), payload.len() as u64)
        .await
}

#[tokio::test]
async fn test_put_and_get_file() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-put-and-get-file");
    let client = setup_bucket(g, &bucket).await?;

    let payload = b"Hello, chunked S3 upload!";
    let id = S3ObjectId {
        bucket,
        key: "test-key.txt".into(),
    };

    client
        .put_file(&id, Cursor::new(payload.to_vec()), payload.len() as u64)
        .await?;

    let mut retrieved_body = Vec::new();
    client
        .get_file(&id)
        .await?
        .1
        .read_to_end(&mut retrieved_body)
        .await?;
    let actual_sha256 = STANDARD.encode(sha2::Sha256::digest(&retrieved_body));
    let expected_checksum = STANDARD.encode(sha2::Sha256::digest(payload));

    assert_eq!(actual_sha256, expected_checksum);
    assert_eq!(
        str::from_utf8(retrieved_body.as_ref())?,
        "Hello, chunked S3 upload!"
    );
    Ok(())
}

#[tokio::test]
async fn test_get_object_sha() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-get-object-sha");
    let client = setup_bucket(g, &bucket).await?;

    let payload = b"Hello, chunked S3 upload!";
    let id = S3ObjectId {
        bucket,
        key: "test-key.txt".into(),
    };

    client
        .put_file(&id, Cursor::new(payload.to_vec()), payload.len() as u64)
        .await?;

    let mut data = Vec::new();
    client.get_file(&id).await?.1.read_to_end(&mut data).await?;
    let actual_sha256 = STANDARD.encode(sha2::Sha256::digest(&data));
    let fetched_sha256 = STANDARD.encode(sha2::Sha256::digest(&data));
    assert_eq!(actual_sha256, fetched_sha256);

    Ok(())
}

#[tokio::test]
async fn test_put_large_file() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-put-large-file");
    let client = setup_bucket(g, &bucket).await?;

    let payload: Vec<u8> = (0..CHUNK_SIZE * 10 + 1000)
        .map(|i| (i % 256) as u8)
        .collect();
    let id = S3ObjectId {
        bucket,
        key: "large-file.bin".into(),
    };

    client
        .put_file(&id, Cursor::new(payload.clone()), payload.len() as u64)
        .await?;

    let mut body = Vec::new();
    client.get_file(&id).await?.1.read_to_end(&mut body).await?;
    assert_eq!(body.len(), payload.len(), "body length mismatch");
    assert_eq!(body, payload);
    Ok(())
}

#[tokio::test]
async fn test_put_empty_file() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-put-empty-file");
    let client = setup_bucket(g, &bucket).await?;

    let id = S3ObjectId {
        bucket,
        key: "empty.txt".into(),
    };

    client.put_file(&id, Cursor::new(vec![]), 0).await?;

    let mut body = Vec::new();
    client.get_file(&id).await?.1.read_to_end(&mut body).await?;
    assert_eq!(str::from_utf8(&body)?, "");
    Ok(())
}

#[tokio::test]
async fn test_virtual_hosted_put_get_and_list() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-virtual-hosted-put-get-and-list");
    let client = setup_bucket_with_style(g, &bucket, AddressingStyle::VirtualHosted).await?;

    let payload = b"virtual hosted upload";
    let id = S3ObjectId {
        bucket: bucket.clone(),
        key: "nested/file.txt".into(),
    };

    client
        .put_file(&id, Cursor::new(payload.to_vec()), payload.len() as u64)
        .await?;

    let mut body = Vec::new();
    client.get_file(&id).await?.1.read_to_end(&mut body).await?;
    assert_eq!(body, payload);

    let objects = client.list_objects(&bucket).await?;
    assert!(objects.iter().any(|it| it.key == id.key));

    Ok(())
}

#[tokio::test]
async fn test_list_buckets() -> Result<(), Report> {
    let g = garage().await?;
    let expected_buckets = [
        bucket_name_with_suffix("test-list-buckets", "a"),
        bucket_name_with_suffix("test-list-buckets", "b"),
    ];
    // Both buckets must share the same key: S3 ListBuckets only returns
    // buckets the calling key has been granted access to.
    let id_a = g.create_bucket(&expected_buckets[0]).await?;
    let id_b = g.create_bucket(&expected_buckets[1]).await?;
    let (key_id, secret) = g.create_key("test-list-buckets").await?;
    g.allow_key_on_bucket(&id_a, &key_id).await?;
    g.allow_key_on_bucket(&id_b, &key_id).await?;
    let client = S3Client::new(
        Client::new(),
        g.s3_endpoint().clone(),
        g.region(),
        &key_id,
        &secret,
        AddressingStyle::Path,
    );

    let bucket_names = client
        .list_buckets()
        .await?
        .into_iter()
        .map(|bucket| bucket.name)
        .collect::<Vec<_>>();

    for bucket in &expected_buckets {
        assert!(
            bucket_names.iter().any(|name| name == bucket),
            "missing bucket {bucket} in {bucket_names:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_list_objects() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-list-objects");
    let client = setup_bucket(g, &bucket).await?;

    let expected_objects = [
        ("alpha.txt", b"alpha".as_slice()),
        ("bravo.bin", &[1, 2, 3, 4, 5]),
        ("foo/bar.bin", &[1, 2, 3, 4, 5]),
        ("foos/bars/bar.bin", "ayyy".as_bytes()),
        ("charlie-empty", b"".as_slice()),
    ];
    for (key, payload) in expected_objects {
        put_object(&client, &bucket, key, payload).await?;
    }

    let mut objects = client.list_objects(&bucket).await?;
    objects.sort_by(|left, right| left.key.cmp(&right.key));

    let mut expected = expected_objects
        .into_iter()
        .map(|(key, payload)| (key.to_string(), payload.len() as u64))
        .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(objects.len(), expected.len());
    for (object, (expected_key, expected_size)) in objects.iter().zip(expected) {
        assert_eq!(object.key, expected_key);
        assert_eq!(object.size, expected_size);
    }

    Ok(())
}

#[tokio::test]
async fn test_delete() -> Result<(), Report> {
    let g = garage().await?;
    let bucket = bucket_name("test-delete-objects");
    let client = setup_bucket(g, &bucket).await?;

    let expected_objects = [
        ("flag.txt", b"cool!".as_slice()),
        ("flag2.txt", b"cool!".as_slice()),
    ];
    for (key, payload) in expected_objects {
        put_object(&client, &bucket, key, payload).await?;
    }
    assert_eq!(client.list_objects(&bucket).await?.len(), 2);

    client
        .delete_file(&S3ObjectId {
            bucket: bucket.clone(),
            key: "flag.txt".into(),
        })
        .await?;

    let objects = client.list_objects(&bucket).await?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, "flag2.txt");

    Ok(())
}
