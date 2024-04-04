pub struct UnstructuredHolder {
    raw_data: Vec<u8>,
}

impl Default for UnstructuredHolder {
    fn default() -> Self {
        Self::new()
    }
}

impl UnstructuredHolder {
    #[must_use]
    pub fn new() -> Self {
        let len = madsim::rand::random::<u16>() as usize;
        let mut raw_data = Vec::with_capacity(len);
        while raw_data.len() < len {
            raw_data.push(madsim::rand::random::<u8>());
        }
        Self { raw_data }
    }

    #[must_use]
    pub fn unstructured(&self) -> arbitrary::Unstructured<'_> {
        arbitrary::Unstructured::new(&self.raw_data)
    }
}
