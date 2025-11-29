use rand::{Rng, SeedableRng as _, rngs::StdRng};

pub struct UnstructuredHolder {
    raw_data: Vec<u8>,
}

impl UnstructuredHolder {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut seedable_rng = StdRng::seed_from_u64(seed);
        let len = seedable_rng.random::<u16>() as usize;
        let mut raw_data = Vec::with_capacity(len);
        while raw_data.len() < len {
            raw_data.push(seedable_rng.random::<u8>());
        }
        Self { raw_data }
    }

    #[must_use]
    pub fn unstructured(&self) -> arbitrary::Unstructured<'_> {
        arbitrary::Unstructured::new(&self.raw_data)
    }
}
