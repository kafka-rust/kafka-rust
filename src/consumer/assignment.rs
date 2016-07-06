//! Encapsulates topic partition assignments to a consumer. Once
//! constructed these assignments are not modified.

use std::collections::HashMap;
use std::ops::Index;
use std::u32;

/// A read-only configuration for `Consumer`.
#[derive(Debug)]
pub struct Assignment {
    /// ~ name of the topic to consume
    topic: String,
    /// ~ list of partitions to consumer, empty if consuming all
    /// available partitions
    /// ~ kept in ascending order
    partitions: Vec<i32>, 
    /// ~ the indirect reference to this assignment through the
    /// hosting config instance
    _ref: AssignmentRef,
}

impl Assignment {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    pub fn _ref(&self) -> AssignmentRef {
        self._ref
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
        self.0.binary_search_by(|x| x.topic.as_str().cmp(topic))
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
        partitions.sort();
        xs.push(Assignment {
            topic: topic,
            partitions: partitions,
            _ref: AssignmentRef(u32::MAX), // ~ something invalid
        });
    }
    xs.sort_by(|a, b| a.topic.cmp(&b.topic));
    // ~ now that the order is set, update the `_refs`s
    for (i, x) in xs.iter_mut().enumerate() {
        x._ref = AssignmentRef(i as u32);
    }
    Assignments(xs)
}
