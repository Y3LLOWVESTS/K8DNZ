cargo build
cargo test -p k8dnz-cli --bin apex_law_program
cargo run -p k8dnz-cli --bin apex_law_program -- build --recipe configs/tuned_validated.k8r --in text/Genesis1.txt --in text/Genesis2.txt --window-bytes 256 --step-bytes 256 --max-windows 12 --seed-count 64 --chunk-sweep 32,64 --boundary-band-sweep 8,12 --field-margin-sweep 4,8 --newline-demote-margin-sweep 0,4 --local-chunk-sweep 32,64,96,128 --tune-default-body --default-body-chunk-sweep 64,96,128 --body-select-objective selected-total --emit-body-scoreboard --min-override-gain-exact 1 --exact-subset-limit 20 --out /tmp/g12_law_program.aklp --out-report /tmp/g12_law_program_build.txt
cargo run -p k8dnz-cli --bin apex_law_program -- replay --artifact /tmp/g12_law_program.aklp --compare-surfaces --out-report /tmp/g12_law_program_replay.txt
cat /tmp/g12_law_program_build.txt
cat /tmp/g12_law_program_replay.txt
cargo run -p k8dnz-cli --bin apex_law_program -- build --recipe configs/tuned_validated.k8r --in text/Genesis1.txt --window-bytes 256 --step-bytes 256 --max-windows 12 --seed-count 64 --chunk-sweep 32,64 --boundary-band-sweep 8,12 --field-margin-sweep 4,8 --newline-demote-margin-sweep 0,4 --local-chunk-sweep 32,64,96,128 --tune-default-body --default-body-chunk-sweep 64,96,128 --body-select-objective selected-total --emit-body-scoreboard --min-override-gain-exact 1 --exact-subset-limit 20 --out /tmp/g1_law_program.aklp --out-report /tmp/g1_law_program_build.txt
cargo run -p k8dnz-cli --bin apex_law_program -- replay --artifact /tmp/g1_law_program.aklp --compare-surfaces --out-report /tmp/g1_law_program_replay.txt
cat /tmp/g1_law_program_build.txt
cat /tmp/g1_law_program_replay.txt
