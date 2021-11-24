#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, Hash)]
#[repr(C)]
pub struct UfoId(pub(crate) u64);

impl UfoId {
    pub fn sentinel() -> Self {
        UfoId(0)
    }

    pub fn is_sentinel(&self) -> bool {
        0 == self.0
    }
}

pub struct UfoIdGen {
    current: u64,
}

impl UfoIdGen {
    pub fn new() -> UfoIdGen {
        UfoIdGen { current: 0 }
    }

    pub(crate) fn next<P>(&mut self, is_unused: P) -> UfoId
    where
        P: Fn(&UfoId) -> bool,
    {
        let mut n = self.current;
        let mut id;
        loop {
            n = n.wrapping_add(1);
            id = UfoId(n);
            if is_unused(&id) {
                break;
            }
        }
        self.current = n;
        id
    }
}
