pub mod wit_highlighter;

use std::rc::Rc;

pub fn trace_id() -> Rc<str> {
    use rand::SeedableRng;
    let mut rng = rand::rngs::SmallRng::from_entropy();
    Rc::from(
        (0..5)
            .map(|_| (rand::Rng::gen_range(&mut rng, b'a'..=b'z') as char))
            .collect::<String>(),
    )
}
