These fixtures contain a generated 440 Hz tone, 0.25 seconds long, at 48 kHz in stereo.
They contain no downloaded media. They exercise WebM/Opus, MP4/AAC with the index at
the front, and fragmented MP4/AAC through Auxide's network reader and decoder.

Regenerate inside `nix develop`:

```sh
ffmpeg -v error -f lavfi -i sine=frequency=440:sample_rate=48000 -t 0.25 -ac 2 \
  -c:a libopus -b:a 64000 -fflags +bitexact -flags:a +bitexact -y tests/fixtures/tone.webm
ffmpeg -v error -f lavfi -i sine=frequency=440:sample_rate=48000 -t 0.25 -ac 2 \
  -c:a aac -b:a 128000 -fflags +bitexact -flags:a +bitexact -movflags +faststart \
  -y tests/fixtures/tone.m4a
ffmpeg -v error -f lavfi -i sine=frequency=440:sample_rate=48000 -t 0.25 -ac 2 \
  -c:a aac -b:a 128000 -fflags +bitexact -flags:a +bitexact \
  -movflags +frag_keyframe+empty_moov+default_base_moof -frag_duration 100000 \
  -y tests/fixtures/tone-fragmented.m4a
```
