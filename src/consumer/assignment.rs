//! Encapsulates topic partition assignments to a consumer. Once
//! constructed these assignments are not modified.

use std::collections::HashMap;
use std::ops::Index;

/// A read-only configuration for `Consumer`.
#[derive(Debug)]
pub struct Assignment {
    /// ~ name of the topic to consume
    topic: String,
    /// ~ list of partitions to consumer, empty if consuming all
    /// available partitions
    /// ~ kept in ascending order
    /// ~ no duplicates
    partitions: Vec<i32>,
}

impl Assignment {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// A "pointer" to an assignment stored in `Config`.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct AssignmentRef(u32);

/// A set of assignments.
#[derive(Debug)]
pub struct Assignments(Vec<Assignment>);

impl Assignments {
    pub fn as_slice(&self) -> &[Assignment] {
        &self.0
    }

    pub fn topic_ref(&self, topic: &str) -> Option<AssignmentRef> {
        self.0
            .binary_search_by(|x| x.topic.as_str().cmp(topic))
            .ok()
            .map(|i| AssignmentRef(i as u32))
    }
}

impl Index<AssignmentRef> for Assignments {
    type Output = Assignment;
    fn index(&self, index: AssignmentRef) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

pub fn from_map(src: HashMap<String, Vec<i32>>) -> Assignments {
    let mut xs = Vec::with_capacity(src.len());
    for (topic, mut partitions) in src {
        partitions.sort_unstable();
        partitions.dedup();
        xs.push(Assignment { topic, partitions });
    }
    // ~ sort by topic such has we can apply binary search by that
    // attribute later
    xs.sort_by(|a, b| a.topic.cmp(&b.topic));
    Assignments(xs)
}
