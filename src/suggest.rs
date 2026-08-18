//! Search results offered while somebody is still typing.
//!
//! Autocomplete is the one Discord interaction that arrives on a keystroke
//! rather than on a decision, and it wants an answer inside three seconds. A
//! search here costs a yt-dlp process, so answering each keystroke by searching
//! would spend a process per letter and queue playback behind the typing.
//!
//! So nothing here ever waits for a search. A query already searched is
//! answered from memory; one that has not been is answered with nothing and
//! searched in the background, which puts results in front of the next
//! keystroke. Typing at any human speed produces suggestions within a letter or
//! two, and a request that would have been slow is simply empty instead — which
//! is what every version before this one did for all of them.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, Semaphore};

use crate::source::{SourceResolver, TrackMetadata};

/// How long a search stays worth offering.
///
/// Long enough to cover somebody typing a query out and correcting it, short
/// enough that a video taken down between the suggestion and the choice is a
/// rarity rather than a routine failure.
const CACHE_TTL: Duration = Duration::from_secs(60);

/// Queries remembered at once, across every server.
const MAX_CACHED: usize = 256;

/// The range of query lengths worth spending a search on.
///
/// One or two letters match everything, so the results are noise and the
/// process spent on them is wasted. The upper bound matches the option's own.
const SEARCHABLE_CHARS: std::ops::RangeInclusive<usize> = 3..=200;

/// Discord's limit on both halves of a choice.
const MAX_CHOICE_CHARS: usize = 100;

struct Cached {
    at: Instant,
    tracks: Vec<TrackMetadata>,
}

/// Answers autocomplete from memory, and fills that memory in the background.
pub struct Suggestions {
    resolver: Arc<dyn SourceResolver>,
    searched: Arc<Mutex<HashMap<String, Cached>>>,
    /// One background search at a time, across every server.
    ///
    /// Held with `try_acquire`, never awaited: typing must not be able to
    /// queue up work, and must never be the reason a track waits to start.
    warming: Arc<Semaphore>,
}

impl Suggestions {
    #[must_use]
    pub fn new(resolver: Arc<dyn SourceResolver>) -> Self {
        Self {
            resolver,
            searched: Arc::new(Mutex::new(HashMap::new())),
            warming: Arc::new(Semaphore::new(1)),
        }
    }

    /// What to offer for a query, right now, without waiting for anything.
    pub async fn for_query(&self, query: &str) -> Vec<(String, String)> {
        let Some(key) = normalize(query) else {
            return Vec::new();
        };

        let mut searched = self.searched.lock().await;
        searched.retain(|_, cached| cached.at.elapsed() < CACHE_TTL);
        let exact = searched.contains_key(&key);
        let offered = best_match(&searched, &key)
            .map(|cached| cached.tracks.iter().map(choice).collect())
            .unwrap_or_default();
        drop(searched);

        // Searched even when something was offered, because what was offered
        // was for less than has been typed. The exact query is what the next
        // keystroke will want, and this is the only chance to have it ready.
        if !exact {
            self.warm(key);
        }
        offered
    }

    /// Searches for a query nobody has searched for yet, if there is room.
    ///
    /// Giving up rather than queueing is the point: a permit that is not free
    /// means a search is already running, and one more keystroke is not worth
    /// making a track wait behind it.
    fn warm(&self, key: String) {
        let Ok(permit) = Arc::clone(&self.warming).try_acquire_owned() else {
            return;
        };
        let resolver = Arc::clone(&self.resolver);
        let searched = Arc::clone(&self.searched);
        tokio::spawn(async move {
            let found = resolver.search(&key).await;
            drop(permit);
            match found {
                Ok(tracks) => {
                    let mut searched = searched.lock().await;
                    // Room is made rather than the result thrown away. Every
                    // keystroke is a query of its own, so a full table is the
                    // ordinary state after a couple of searches rather than a
                    // sign of anything — and refusing to remember any more
                    // would have turned suggestions off for a minute at a time,
                    // right after the typing that filled it.
                    while searched.len() >= MAX_CACHED {
                        let Some(oldest) = searched
                            .iter()
                            .min_by_key(|(_, cached)| cached.at)
                            .map(|(key, _)| key.clone())
                        else {
                            break;
                        };
                        searched.remove(&oldest);
                    }
                    searched.insert(
                        key,
                        Cached {
                            at: Instant::now(),
                            tracks,
                        },
                    );
                }
                // A failed search is not worth remembering or reporting: the
                // person is still typing, and the picker behind `/play` will
                // say why if they go through with it.
                Err(error) => tracing::debug!(%error, "a suggestion search failed"),
            }
        });
    }
}

/// The best thing already known for what somebody has typed so far.
///
/// An exact hit if there is one, and otherwise the longest query already
/// searched that this one begins with. Somebody midway through typing
/// `ella langley` has `ella lang` in hand, and those are the same results.
///
/// Without this there are almost never any suggestions at all, which is what
/// the feature looked like in use. Every keystroke is a different query, so the
/// search one keystroke starts lands under a key the next keystroke has already
/// moved past — and the last keystroke, the only one whose answer anybody sees,
/// is by definition the one nothing has searched yet. Exact matching could only
/// pay off for somebody who typed a letter and deleted it again.
fn best_match<'a>(searched: &'a HashMap<String, Cached>, key: &str) -> Option<&'a Cached> {
    searched
        .iter()
        .filter(|(searched_key, _)| key.starts_with(searched_key.as_str()))
        .max_by_key(|(searched_key, _)| searched_key.len())
        .map(|(_, cached)| cached)
}

/// Reduces a query to what makes two of them the same search.
///
/// Returns `None` for anything too short or too long to be worth searching.
fn normalize(query: &str) -> Option<String> {
    let key = query.split_whitespace().collect::<Vec<_>>().join(" ");
    let length = key.chars().count();
    SEARCHABLE_CHARS
        .contains(&length)
        .then(|| key.to_lowercase())
}

/// Turns a track into what Discord shows and what it submits.
///
/// The value is the canonical link rather than the title, so choosing a
/// suggestion takes the URL path through `/play`: no second search, no picker,
/// and an answer the whole channel sees.
fn choice(track: &TrackMetadata) -> (String, String) {
    let duration = track.duration.as_secs();
    let suffix = format!(" ({}:{:02})", duration / 60, duration % 60);
    let room = MAX_CHOICE_CHARS.saturating_sub(suffix.chars().count());
    let mut name = track
        .title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(room)
        .collect::<String>();
    name.push_str(&suffix);
    (name, track.canonical_url.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use url::Url;

    use super::*;

    fn track(title: &str, seconds: u64) -> TrackMetadata {
        TrackMetadata {
            source_id: "abc".to_owned(),
            canonical_url: Url::parse("https://www.youtube.com/watch?v=abc").unwrap(),
            title: title.to_owned(),
            channel: None,
            duration: Duration::from_secs(seconds),
            thumbnail_url: None,
        }
    }

    /// Counts searches, and can be made slow enough to prove nothing waits.
    struct CountingSource {
        searches: Arc<std::sync::atomic::AtomicUsize>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl SourceResolver for CountingSource {
        async fn search(
            &self,
            query: &str,
        ) -> Result<Vec<TrackMetadata>, crate::source::SourceError> {
            self.searches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(vec![track(&format!("{query} result"), 90)])
        }
        async fn inspect(&self, _url: &Url) -> Result<TrackMetadata, crate::source::SourceError> {
            unreachable!()
        }
        async fn resolve(
            &self,
            _track: &TrackMetadata,
        ) -> Result<crate::source::ResolvedAudio, crate::source::SourceError> {
            unreachable!()
        }
        fn accepts(&self, _url: &Url) -> Result<(), crate::source::SourceError> {
            unreachable!()
        }
        async fn playlist(
            &self,
            _url: &Url,
        ) -> Result<Option<crate::source::Playlist>, crate::source::SourceError> {
            unreachable!()
        }
    }

    fn counting(delay: Duration) -> (Suggestions, Arc<std::sync::atomic::AtomicUsize>) {
        let searches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = CountingSource {
            searches: Arc::clone(&searches),
            delay,
        };
        (Suggestions::new(Arc::new(source)), searches)
    }

    #[tokio::test]
    async fn a_keystroke_is_answered_from_memory_or_not_at_all() {
        let (suggestions, searches) = counting(Duration::ZERO);

        // Nothing has been searched for, so this answers empty rather than
        // making somebody wait mid-word.
        assert!(suggestions.for_query("rick astley").await.is_empty());
        tokio::time::sleep(Duration::from_millis(50)).await;

        // And the search it started is what the next keystroke reads.
        let offered = suggestions.for_query("rick  ASTLEY ").await;
        assert_eq!(offered.len(), 1);
        assert!(offered[0].0.starts_with("rick astley result"));
        assert_eq!(
            offered[0].1, "https://www.youtube.com/watch?v=abc",
            "choosing a suggestion must take the URL path through /play"
        );
        // Spacing and case were the same search, so it only ran once.
        assert_eq!(searches.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn typing_never_waits_for_a_search_or_queues_them_up() {
        let (suggestions, searches) = counting(Duration::from_millis(400));

        // Ten keystrokes against a search slower than Discord's own budget.
        let started = Instant::now();
        for length in 3..=12 {
            let query = "abcdefghijkl"[..length].to_owned();
            assert!(suggestions.for_query(&query).await.is_empty());
        }
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "answering keystrokes waited on a search"
        );

        // Ten keystrokes, and by the time the first search finishes only that
        // one has run: typing cannot queue work up behind itself, and so cannot
        // crowd out a track waiting to start.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(searches.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn what_was_searched_a_keystroke_ago_answers_what_is_being_typed_now() {
        let (suggestions, _) = counting(Duration::ZERO);

        // Typing out a query, one keystroke at a time, is the only way anybody
        // ever uses this — and every keystroke is a different query. Answering
        // only exact matches meant the last keystroke, the one whose answer is
        // actually seen, was always the one nothing had searched, so the whole
        // feature read as "No options match your search" no matter what was
        // typed.
        assert!(suggestions.for_query("ella").await.is_empty());
        tokio::time::sleep(Duration::from_millis(50)).await;

        for typed in ["ella l", "ella lan", "ella langley"] {
            let offered = suggestions.for_query(typed).await;
            assert_eq!(offered.len(), 1, "nothing offered for {typed:?}");
            // Whatever answers has to be a search for something this query
            // begins with — never a leftover from some unrelated word. Which
            // prefix it is depends on how many background searches have landed,
            // and a longer one taking over as it arrives is the point.
            let answered = offered[0]
                .0
                .strip_suffix(" result (1:30)")
                .unwrap_or_else(|| panic!("unexpected suggestion {:?}", offered[0].0));
            assert!(
                typed.starts_with(answered),
                "{typed:?} was answered with a search for {answered:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // And the longer query, once its own search has landed, wins over the
        // shorter one it was standing in for.
        let offered = suggestions.for_query("ella langley").await;
        assert!(
            offered[0].0.starts_with("ella langley result"),
            "a stale prefix outlived the exact search: {:?}",
            offered[0].0
        );
    }

    #[tokio::test]
    async fn a_full_table_still_learns_what_is_being_typed_now() {
        let (suggestions, _) = counting(Duration::ZERO);

        // Filling it takes only a few words typed out, because each keystroke
        // is its own entry. Refusing to search once full meant suggestions
        // stopped entirely for the next minute.
        for index in 0..MAX_CACHED + 20 {
            suggestions
                .for_query(&format!("query number {index}"))
                .await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let offered = suggestions.for_query("something entirely new").await;
        assert!(offered.is_empty(), "a query nobody typed before was cached");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !suggestions
                .for_query("something entirely new")
                .await
                .is_empty(),
            "a full table stopped learning"
        );
    }

    #[test]
    fn two_ways_of_typing_the_same_query_are_one_search() {
        assert_eq!(
            normalize("  Rick   Astley  "),
            normalize("rick astley"),
            "spacing and case made two searches out of one"
        );
    }

    #[test]
    fn a_query_too_short_to_narrow_anything_is_not_searched() {
        assert_eq!(normalize("r"), None);
        assert_eq!(normalize("ri"), None);
        assert!(normalize("ric").is_some());
        assert_eq!(normalize("   "), None);
        // The command itself caps the option, but a query this long is a
        // mistake rather than a search worth spending a process on.
        assert_eq!(normalize(&"x".repeat(201)), None);
    }

    #[test]
    fn a_choice_fits_what_discord_will_accept() {
        let (name, value) = choice(&track("A Reasonable Title", 225));
        assert_eq!(name, "A Reasonable Title (3:45)");
        assert_eq!(value, "https://www.youtube.com/watch?v=abc");

        // A title long enough to overflow is cut so the duration survives,
        // because the duration is the part that distinguishes two uploads of
        // the same song.
        let (name, _) = choice(&track(&"x".repeat(300), 65));
        assert!(name.chars().count() <= MAX_CHOICE_CHARS);
        assert!(name.ends_with("(1:05)"));
    }
}
