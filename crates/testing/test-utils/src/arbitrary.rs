pub struct UnstructuredHolder {
    raw_data: Vec<u8>,
}

impl UnstructuredHolder {
    pub fn new() -> Self {
        let len = madsim::rand::random::<u16>() as usize;
        let mut raw_data = Vec::with_capacity(len);
        while raw_data.len() < len {
            raw_data.push(madsim::rand::random::<u8>());
        }
        Self { raw_data }
    }

    pub fn unstructured(&self) -> arbitrary::Unstructured<'_> {
        arbitrary::Unstructured::new(&self.raw_data)
    }
}
