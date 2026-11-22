use crate::data::is_addressable_key;
use rootcause::{Report, bail, report};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressingStyle {
    Path,
    VirtualHosted,
    VirtualHostedResolveDns,
}

impl AddressingStyle {
    pub fn format_url(
        &self,
        base_url: &Url,
        bucket: &str,
        key: Option<&str>,
    ) -> Result<ForwardObjectUrl, Report> {
        if let Some(key) = key
            && !is_addressable_key(key)
        {
            bail!("key cannot be addressed in a URL: {key:?}");
        }
        let mut url = base_url.clone();
        let host = base_url.host_str().ok_or_else(|| {
            report!("base URL must have a host").attach(format!("url: {base_url}"))
        })?;
        let host = format!("{bucket}.{host}");
        let mut segments = url.path_segments_mut().map_err(|()| {
            report!("base URL cannot be cannot-be-a-base").attach(format!("url: {base_url}"))
        })?;

        Ok(match self {
            Self::Path => {
                segments.push(bucket);
                if let Some(key) = key {
                    segments.extend(key.split('/'));
                }
                drop(segments);
                ForwardObjectUrl::no_host(url)
            }
            Self::VirtualHosted => {
                if let Some(key) = key {
                    segments.extend(key.split('/'));
                }
                drop(segments);
                ForwardObjectUrl::with_host(url, host)
            }
            Self::VirtualHostedResolveDns => {
                if let Some(key) = key {
                    segments.extend(key.split('/'));
                }
                drop(segments);
                url.set_host(Some(&host)).map_err(|err| {
                    report!("failed to set virtual hosted endpoint").attach(format!("error: {err}"))
                })?;
                ForwardObjectUrl::no_host(url)
            }
        })
    }

    pub fn format_bucket_url(
        &self,
        base_url: &Url,
        bucket: &str,
    ) -> Result<ForwardObjectUrl, Report> {
        self.format_url(base_url, bucket, None)
    }
}

pub struct ForwardObjectUrl {
    pub url: Url,
    pub host_header: Option<String>,
}

impl Display for ForwardObjectUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(host_header) = &self.host_header {
            write!(f, "{} (host: {})", self.url, host_header)
        } else {
            write!(f, "{}", self.url)
        }
    }
}

impl ForwardObjectUrl {
    pub fn no_host(url: Url) -> Self {
        Self {
            url,
            host_header: None,
        }
    }

    pub fn with_host(url: Url, host_header: String) -> Self {
        Self {
            url,
            host_header: Some(host_header),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AddressingStyle;
    use crate::s3::crypto::canonical_uri;
    use url::Url;

    fn path(key: &str) -> Option<String> {
        let base = Url::parse("http://localhost:3900/").unwrap();
        AddressingStyle::Path
            .format_url(&base, "b", Some(key))
            .ok()
            .map(|it| it.url.path().to_string())
    }

    #[test]
    fn formats_keys_exactly() {
        let cases = [
            ("k", "/b/k"),
            ("a//b", "/b/a//b"),
            ("a/", "/b/a/"),
            ("/a", "/b//a"),
            ("a b+c%d", "/b/a%20b+c%25d"),
            ("ä?#", "/b/%C3%A4%3F%23"),
            ("a/.b/..c", "/b/a/.b/..c"),
        ];
        for (key, expected) in cases {
            assert_eq!(path(key).as_deref(), Some(expected), "key: {key}");
        }
    }

    #[test]
    fn rejects_dot_segments() {
        for key in [
            "a/./b", "a/../b", "./a", "..", "a/.", "a\tb", "a\nb", "a\rb",
        ] {
            assert_eq!(path(key), None, "key: {key}");
        }
    }

    #[test]
    fn signs_the_forwarded_path() {
        let base = Url::parse("http://localhost:3900/").unwrap();
        let url = AddressingStyle::Path
            .format_url(&base, "b", Some("a b/+ä//c"))
            .unwrap()
            .url;
        assert_eq!(canonical_uri(&url), "/b/a%20b/%2B%C3%A4//c");
    }
}
