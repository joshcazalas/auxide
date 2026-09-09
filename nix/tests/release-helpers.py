import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


BUILD_SCRIPT, PUBLISH_SCRIPT = sys.argv[1:3]
sys.argv[1:] = []

FAKE_TOOL = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["TEST_CALLS"], "a") as log:
    log.write(json.dumps([tool, *args]) + "\n")
operation = tool + ":" + (args[1] if tool == "gh" else args[0])
if os.environ.get("TEST_FAIL") in (tool, operation):
    print("simulated " + operation + " failure", file=sys.stderr)
    sys.exit(41)

root = Path(os.environ["TEST_ROOT"])
if tool == "nix":
    if args == ["build", "--no-link", "--print-out-paths", ".#auxide"]:
        print(root / "package")
    elif args == ["build", "--no-link", "--print-out-paths", ".#oci-image"]:
        print(root / "image.tar.gz")
    elif args[:2] == ["path-info", "--derivation"]:
        print(args[2] + ".drv")
    elif args == ["flake", "metadata", "--json"]:
        print(json.dumps({"locked": {"rev": os.environ["GITHUB_SHA"]}}))
    elif args[:2] == ["path-info", "--json"]:
        print(json.dumps({str(root / "package"): {"closureSize": 123}}))
    else:
        sys.exit("unexpected nix command: " + repr(args))
elif tool == "sbomnix":
    assert args[0] == ".#auxide"
    for flag, data in [
        ("--cdx", {"bomFormat": "CycloneDX", "specVersion": "1.6"}),
        ("--spdx", {"spdxVersion": "SPDX-2.3"}),
    ]:
        Path(args[args.index(flag) + 1]).write_text(json.dumps(data))
    Path(args[args.index("--csv") + 1]).write_text("name,version\nauxide,0.1.0\n")
elif tool == "provenance":
    Path(args[args.index("--out") + 1]).write_text(json.dumps({"subject": args[0]}))
elif tool == "gh":
    assert args[0] == "release"
    assert args[2] == "auxide-test-release"
    if args[1] == "view":
        state = os.environ.get("TEST_RELEASE", "missing")
        if state == "missing":
            sys.exit(1)
        if "--json" in args:
            print("true" if state == "draft" else "false")
    elif args[1] not in ["create", "upload", "edit"]:
        sys.exit("unexpected gh command: " + repr(args))
else:
    sys.exit("unexpected tool: " + tool)
'''


class ReleaseHelpers(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="auxide release tests ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repository"
        self.repo.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for tool in ["nix", "gh", "sbomnix", "provenance"]:
            path = self.bin / tool
            path.write_text(FAKE_TOOL.replace("#!/usr/bin/env python3", "#!" + sys.executable, 1))
            path.chmod(0o755)
        self.calls_file = self.root / "calls.jsonl"
        self.env = {
            **os.environ,
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Release fixture",
            "GIT_COMMITTER_NAME": "Release fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "TEST_ROOT": str(self.root),
            "TEST_CALLS": str(self.calls_file),
            "TEST_FAIL": "",
            "TEST_RELEASE": "missing",
        }
        self.run_command(["git", "init", "-q"])
        for name in ["Cargo.lock", "flake.lock"]:
            (self.repo / name).write_text(name + " fixture\n")
        self.run_command(["git", "add", "."])
        self.run_command(["git", "commit", "-qm", "Fixture"])
        self.env["GITHUB_SHA"] = self.run_command(["git", "rev-parse", "HEAD"]).stdout.strip()
        (self.root / "package").mkdir()
        (self.root / "image.tar.gz").write_bytes(b"fixture OCI archive\n")
        self.bundle = self.repo / ".release" / "bundle with spaces"

    def run_command(self, args, success=True):
        result = subprocess.run(
            args, cwd=self.repo, env=self.env, text=True, capture_output=True, timeout=30
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def build(self, success=True):
        return self.run_command(
            ["bash", BUILD_SCRIPT, "auxide-test-release", str(self.bundle)], success
        )

    def publish(self, success=True):
        return self.run_command(
            ["bash", PUBLISH_SCRIPT, "auxide-test-release", str(self.bundle)], success
        )

    def calls(self, tool=None):
        if not self.calls_file.exists():
            return []
        calls = [json.loads(line) for line in self.calls_file.read_text().splitlines()]
        return [call for call in calls if tool is None or call[0] == tool]

    def test_bundle_preserves_identity_and_hashes_every_artifact(self):
        self.build()
        manifest = json.loads((self.bundle / "manifest.json").read_text())
        self.assertEqual(manifest["source"]["commit"], self.env["GITHUB_SHA"])
        self.assertEqual(manifest["release"], "auxide-test-release")
        self.assertEqual(manifest["outputs"]["package"]["storePath"], str(self.root / "package"))
        self.assertEqual(
            manifest["outputs"]["ociImage"]["derivation"], str(self.root / "image.tar.gz.drv")
        )
        for name in ["Cargo.lock", "flake.lock"]:
            self.assertEqual(
                (self.bundle / name).read_bytes(), (self.repo / name).read_bytes()
            )
        self.assertEqual(
            (self.bundle / "auxide-oci.tar.gz").read_bytes(),
            (self.root / "image.tar.gz").read_bytes(),
        )
        for name in [
            "auxide.cdx.json",
            "auxide.spdx.json",
            "auxide-provenance.json",
            "auxide-closure.json",
            "flake-metadata.json",
        ]:
            self.assertTrue(json.loads((self.bundle / name).read_text()))
        sums = {}
        for line in (self.bundle / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ", 1)
            sums[name] = digest
            self.assertEqual(
                digest, hashlib.sha256((self.bundle / name).read_bytes()).hexdigest()
            )
        self.assertEqual(set(sums), {p.name for p in self.bundle.iterdir()} - {"SHA256SUMS"})
        self.assertEqual(self.calls("gh"), [])

    def test_dirty_source_is_refused_before_building(self):
        (self.repo / "Cargo.lock").write_text("changed\n")
        result = self.build(success=False)
        self.assertIn("dirty tracked worktree", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_artifact_tool_failures_stop_bundle_creation(self):
        for tool in ["nix", "sbomnix", "provenance"]:
            with self.subTest(tool=tool):
                shutil.rmtree(self.bundle, ignore_errors=True)
                self.env["TEST_FAIL"] = tool
                result = self.build(success=False)
                self.assertIn("simulated", result.stderr)
                self.assertFalse((self.bundle / "SHA256SUMS").exists())

    def test_missing_bundle_never_calls_github(self):
        self.publish(success=False)
        self.assertEqual(self.calls("gh"), [])

    def test_missing_or_empty_checksums_never_call_github(self):
        self.bundle.mkdir(parents=True)
        self.publish(success=False)
        (self.bundle / "SHA256SUMS").write_text("")
        self.publish(success=False)
        self.assertEqual(self.calls("gh"), [])

    def test_changed_and_missing_artifacts_block_publication(self):
        self.build()
        image = self.bundle / "auxide-oci.tar.gz"
        image.write_bytes(b"changed")
        self.publish(success=False)
        image.unlink()
        self.publish(success=False)
        self.assertEqual(self.calls("gh"), [])

    def test_new_release_is_created_as_draft_with_exact_commit_and_assets(self):
        self.build()
        self.publish()
        calls = self.calls("gh")
        self.assertEqual([call[2] for call in calls], ["view", "create", "edit"])
        create = calls[1]
        self.assertIn("--draft", create)
        self.assertEqual(create[create.index("--target") + 1], self.env["GITHUB_SHA"])
        self.assertEqual(
            create[create.index("--notes-file") + 1], str(self.bundle / "RELEASE_NOTES.md")
        )
        self.assertEqual(
            {str(p) for p in self.bundle.iterdir()}, set(create[4:create.index("--draft")])
        )
        self.assertIn("--draft=false", calls[-1])

    def test_draft_retry_replaces_assets_before_publishing(self):
        self.build()
        self.env["TEST_RELEASE"] = "draft"
        self.publish()
        calls = self.calls("gh")
        self.assertEqual([call[2] for call in calls], ["view", "view", "upload", "edit"])
        self.assertIn("--clobber", calls[2])

    def test_published_release_is_not_modified(self):
        self.build()
        self.env["TEST_RELEASE"] = "published"
        self.publish()
        self.assertEqual([call[2] for call in self.calls("gh")], ["view", "view"])

    def test_failed_upload_cannot_publish_a_draft(self):
        self.build()
        self.env.update(TEST_RELEASE="draft", TEST_FAIL="gh:upload")
        self.publish(success=False)
        self.assertNotIn("edit", [call[2] for call in self.calls("gh")])

    def test_failed_create_cannot_publish_a_release(self):
        self.build()
        self.env["TEST_FAIL"] = "gh:create"
        self.publish(success=False)
        self.assertNotIn("edit", [call[2] for call in self.calls("gh")])

    def test_authentication_and_publication_failures_are_reported(self):
        self.build()
        for failure in ["gh", "gh:edit"]:
            with self.subTest(failure=failure):
                self.env["TEST_FAIL"] = failure
                self.assertIn("simulated", self.publish(success=False).stderr)


unittest.main(verbosity=2)
