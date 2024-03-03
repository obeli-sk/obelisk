static INIT: std::sync::Once = std::sync::Once::new();
pub(crate) fn set_up() {
    INIT.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().without_time())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    });
}

#[cfg(test)]
mod tests {

    pub(crate) fn arbtest_seed(mut random: u64, iteration: usize) -> u64 {
        let mut rng = std::iter::repeat_with(move || {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            random
        })
        .skip(iteration);
        let random = rng.next().unwrap();
        // need to generate 16 bits for size and 32 bits for seed.
        let length = random & 0x000000000000FFFF; // randomness stored in a vec of length up to 2^16
        let seed = random & 0xFFFFFFFF00000000; // leave the upper 32 bits.
        let mashed: u64 = length | seed;
        mashed
    }

    #[test]
    fn all_numbers_sync() {
        crate::testing::set_up();
        let builder = tokio::madsim::runtime::Builder::from_env();
        let seed = builder.seed;
        std::thread::spawn(move || {
            let runtime =
                madsim::runtime::Runtime::with_seed_and_config(builder.seed, builder.config);
            arbtest::arbtest(|u| {
                runtime.block_on(async {
                    tracing::info!("{}", madsim::rand::random::<u64>());
                    let number: u32 = u.arbitrary()?;
                    assert!(number % 100 != 0, "failed on {number}");
                    Ok(())
                })
            })
            .seed(arbtest_seed(seed, 0))
            .run();
        })
        .join()
        .inspect_err(|_| {
            eprintln!(
                "note: run with `MADSIM_TEST_SEED={seed}` environment variable to reproduce this error"
            );
        })
        .unwrap();
    }

    #[test]
    fn test_runtime() {
        crate::testing::set_up();
        tracing::info!("init");
        std::thread::spawn(move || {
            let runtime = madsim::runtime::Runtime::with_seed_and_config(1, Default::default());
            runtime.block_on(async {
                tracing::info!("{}", madsim::rand::random::<u64>());
            })
        })
        .join()
        .unwrap();
    }

    // #[test]
    // fn run2() {
    //     crate::testing::set_up();
    //     let builder = tokio::madsim::runtime::Builder::from_env();
    //     builder.run2(|runtime| {
    //         let seed = runtime.handle().seed();
    //         for i in 1..1000 {
    //             arbtest::arbtest(|u| {
    //                 runtime.block_on(async {
    //                     let number: u32 = u.arbitrary()?;
    //                     assert!(number % 1000 != 0, "failed on {number}, iteration {i}");
    //                     Ok(())
    //                 })
    //             })
    //             .seed(crate::testing::arbtest_seed(seed, i))
    //             .run();
    //         }
    //     });
    // }
}
