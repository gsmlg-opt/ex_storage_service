#!/usr/bin/env python3
"""Explicit, network-enabled reference refresh. Ordinary builds never call this."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / 'apps/ex_storage_service_cli/priv/compatibility'
MC_REV = '77f82e18b5401a65958f1619df6ebb994634bd88'
AWS_VERSION = '2.31.18'
AWS_REV = 'f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc'
BASELINE = '9b8661e58107eddb24cb8f15495cee4ac979219a'
ARCHIVES = {
    'awscli.zip': ('https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.31.18.zip', '4a1257f6eca864dcdee1b98e29e9428466ed365398c50032ddf5e4b4d3728e32'),
    'aws-source.tar.gz': ('https://codeload.github.com/aws/aws-cli/tar.gz/refs/tags/2.31.18', '0e164de8e225122efccfae85c989e5b47c2de8a3d1efc4e7bdfe02ac29827d73'),
    'mc.tar.gz': ('https://codeload.github.com/minio/mc/tar.gz/' + MC_REV, '167415edd21bc29f5360943dac64272aa5cda0a39f3070b15cfeca671c43d975'),
}


def digest(path):
    return hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()


def run(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + '\n')


def normalize(text, home, cache):
    return text.replace(str(home), '${HOME}').replace(str(cache), '${REFERENCE_CACHE}')


def capture_aws_notices(release):
    # Preserve the complete license and applicable notice from the locked distro.
    licenses = (release / 'aws/THIRD_PARTY_LICENSES').read_bytes()
    apache = licenses.split(b'\nApache License\n', 1)[1].split(b'\n* For botocore see also this required NOTICE:', 1)[0]
    (DEST / 'aws/LICENSE-APACHE-2.0.txt').write_bytes(b'Apache License\n' + apache)
    botocore = licenses.split(b'* For botocore see also this required NOTICE:\n', 1)[1].split(b'\n** colorama;', 1)[0]
    (DEST / 'aws/NOTICE-BOTOCORE.txt').write_bytes(botocore)
    notices = 'AWS S3 model data, S3/S3Control/STS endpoint data, and reference customization source are from AWS CLI ' + AWS_VERSION + ', licensed under Apache-2.0; see aws/LICENSE.txt, aws/LICENSE-APACHE-2.0.txt, and aws/NOTICE-BOTOCORE.txt.\nMinIO command definitions and help are behavioral reference captures from minio/mc ' + MC_REV + ', whose source is GNU AGPLv3. No MinIO implementation source is imported. ESS remains MIT licensed.\nReference binaries are not shipped and are never invoked by ESS. Python and Go are capture-time tools only.\n'
    (DEST / 'NOTICE.txt').write_text(notices)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, required=True)
    args = parser.parse_args()
    cache = args.cache.resolve()
    cache.mkdir(parents=True, exist_ok=True)
    for name, (url, sha) in ARCHIVES.items():
        path = cache / name
        if not path.exists():
            urllib.request.urlretrieve(url, path)
        if digest(path) != sha:
            raise SystemExit('Reference archive digest mismatch: ' + name)
    release = cache / 'aws-release'
    if not release.exists():
        with zipfile.ZipFile(cache / 'awscli.zip') as archive:
            archive.extractall(release)
        # zipfile does not restore executable permissions.
        (release / 'aws/dist/aws').chmod(0o755)
    for name in ['aws-source.tar.gz', 'mc.tar.gz']:
        with tarfile.open(cache / name) as archive:
            archive.extractall(cache, filter='data')
    aws_source = cache / ('aws-cli-' + AWS_VERSION)
    aws_dist = release / 'aws/dist'
    aws_bin = aws_dist / 'aws'
    mc_source = cache / ('mc-' + MC_REV)
    mc_bin = cache / 'mc'
    ldflags = '-X github.com/minio/mc/cmd.CommitID=' + MC_REV
    run(['go', 'build', '-trimpath', '-buildvcs=false', '-ldflags', ldflags, '-o', str(mc_bin), '.'], cwd=mc_source)
    (mc_source / 'cmd/ess_inventory_test.go').unlink(missing_ok=True)
    (mc_source / 'cmd/ess_inventory.go').write_text((ROOT / 'scripts/cli_compatibility/mc_probe.go.txt').read_text())
    original_main = (mc_source / 'main.go').read_bytes()
    (mc_source / 'main.go').write_text('package main\nimport "github.com/minio/mc/cmd"\nfunc main() { cmd.ESSCapture() }\n')
    probe = cache / 'probe/mc'
    probe.parent.mkdir(exist_ok=True)
    try:
        run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(probe), '.'], cwd=mc_source)
        env = dict(os.environ, ESS_CAPTURE_OUTPUT=str(cache / 'mc-tree.json'), MC_UPDATE='off', MINIO_UPDATE='off')
        run([str(probe)], env=env)
    finally:
        (mc_source / 'main.go').write_bytes(original_main)
        (mc_source / 'cmd/ess_inventory.go').unlink(missing_ok=True)
    home = Path.home()
    tree = json.loads(normalize((cache / 'mc-tree.json').read_text(), home, cache))
    write_json(DEST / 'mc/command-tree.json', tree)
    # mc anonymous dispatches positional actions rather than cli.Subcommands.
    anonymous = mc_source / 'cmd/anonymous-main.go'
    action_source = anonymous.read_text().split('func mainAnonymous(', 1)[1]
    action_switch = action_source.split('switch ctx.Args().First() {', 1)[1].split('default:', 1)[0]
    action_names = sorted(set(re.findall(r'"([a-z]+(?:-[a-z]+)?)"', '\n'.join(re.findall(r'case ([^:]+):', action_switch)))))
    variants = [{'path': ['anonymous', name], 'parent': ['anonymous'], 'source_file': 'cmd/anonymous-main.go', 'source_sha256': digest(anonymous), 'dispatch': 'positional action switch'} for name in action_names]
    write_json(DEST / 'mc/action-variants.json', variants)
    mc_env = dict(os.environ, MC_UPDATE='off', MINIO_UPDATE='off', TERM='dumb', PAGER='cat')
    help_dir = DEST / 'mc/help'
    help_dir.mkdir(exist_ok=True)
    help_results = []
    for row in tree['commands'] + variants:
        path = row['path']
        result = subprocess.run([str(mc_bin), *path, '--help'], env=mc_env, text=True, capture_output=True, timeout=30)
        (help_dir / ('__'.join(path) + '.txt')).write_text(normalize(result.stdout, home, cache))
        if result.stderr:
            (help_dir / ('__'.join(path) + '.stderr.txt')).write_text(normalize(result.stderr, home, cache))
        help_results.append({'path': path, 'exit_code': result.returncode, 'stderr': bool(result.stderr)})
    write_json(DEST / 'mc/help-results.json', help_results)
    (help_dir / 'root.txt').write_text(normalize(run([str(mc_bin), '--help'], env=mc_env), home, cache))
    # Capture the binary's original roff input, independent of host groff version.
    render = cache / 'render-bin'
    render.mkdir(exist_ok=True)
    (render / 'groff').write_text('#!/bin/sh\ncat\n')
    (render / 'groff').chmod(0o755)
    aws_env = dict(os.environ, PATH=str(render) + os.pathsep + os.environ['PATH'], AWS_PAGER='cat', PAGER='cat', AWS_EC2_METADATA_DISABLED='true', AWS_CLI_AUTO_PROMPT='off')
    model_src = aws_dist / 'awscli/botocore/data'
    for path in sorted((model_src / 's3/2006-03-01').glob('*.json')):
        shutil.copyfile(path, DEST / 'aws' / path.name)
    for service, version in [('s3control', '2018-08-20'), ('sts', '2011-06-15')]:
        shutil.copyfile(model_src / service / version / 'endpoint-rule-set-1.json', DEST / 'aws' / (service + '-endpoint-rule-set-1.json'))
    for name in ['partitions.json', 'endpoints.json']:
        shutil.copyfile(model_src / name, DEST / 'aws' / name)
    shutil.copyfile(aws_dist / 'awscli/data/cli.json', DEST / 'aws/cli.json')
    custom = DEST / 'aws/customizations'
    custom.mkdir(exist_ok=True)
    shutil.copytree(aws_source / 'awscli/customizations/s3', custom / 's3', dirs_exist_ok=True)
    for name in ['cliinput.py', 'streamingoutputarg.py', 's3events.py', 's3errormsg.py', 'binaryformat.py', 'binaryhoist.py', 'waiters.py', 'paginate.py', 'generatecliskeleton.py']:
        shutil.copyfile(aws_source / 'awscli/customizations' / name, custom / name)
    shutil.copyfile(aws_source / 'awscli/handlers.py', custom / 'handlers.py')
    for name in ['argprocess.py', 'arguments.py', 'paramfile.py', 'formatter.py', 'clidriver.py']:
        shutil.copyfile(aws_source / 'awscli' / name, custom / name)
    shutil.copyfile(aws_source / 'awscli/customizations/arguments.py', custom / 'custom_arguments.py')
    shutil.copyfile(aws_source / 'LICENSE.txt', DEST / 'aws/LICENSE.txt')
    capture_aws_notices(release)
    model = json.loads((DEST / 'aws/service-2.json').read_text())
    waiters = json.loads((DEST / 'aws/waiters-2.json').read_text())['waiters']
    def kebab(name):
        import re
        return re.sub(r'([a-z0-9])([A-Z])', r'\1-\2', re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1-\2', name)).lower()
    aws_help = DEST / 'aws/help'
    aws_help.mkdir(exist_ok=True)
    paths = [['s3'], ['s3api'], ['s3api', 'wait']]
    paths += [['s3', name] for name in ['ls', 'cp', 'mv', 'rm', 'sync', 'mb', 'rb', 'presign', 'website']]
    paths += [['s3api', kebab(name)] for name in model['operations']]
    paths += [['s3api', 'wait', kebab(name)] for name in waiters]
    for path in paths:
        output = run([str(aws_bin), *path, 'help'], env=aws_env)
        (aws_help / ('__'.join(path) + '.roff')).write_text(output)
    skeletons = DEST / 'aws/skeletons'
    skeletons.mkdir(exist_ok=True)
    for name, operation in model['operations'].items():
        output = model['shapes'].get(operation.get('output', {}).get('shape'), {})
        payload = output.get('members', {}).get(output.get('payload'), {})
        if payload.get('streaming') or name == 'SelectObjectContent':
            continue
        command = kebab(name)
        result = run([str(aws_bin), 's3api', command, '--generate-cli-skeleton', 'input'], env=aws_env)
        (skeletons / (command + '.json')).write_text(result)
    artifacts = {str(path.relative_to(ROOT)): digest(path) for path in sorted(DEST.rglob('*')) if path.is_file()}
    lock = {
        'schema_version': 1, 'capture_date': '2026-10-15', 'ess_source_baseline': BASELINE,
        'platform': 'linux/x86_64', 'capture_policy': 'explicit refresh only; no network during generation, build, or CLI execution',
        'mc': {'repository': 'https://github.com/minio/mc', 'source_revision': MC_REV, 'source_archive_sha256': ARCHIVES['mc.tar.gz'][1], 'binary_sha256': digest(mc_bin), 'binary_version': normalize(run([str(mc_bin), '--version'], env=mc_env).strip(), home, cache), 'build': {'go_version': run(['go', 'version']).strip(), 'flags': ['-trimpath', '-buildvcs=false', '-ldflags', ldflags]}, 'help_format': 'plain text; HOME paths normalized'},
        'aws_cli': {'repository': 'https://github.com/aws/aws-cli', 'version': AWS_VERSION, 'source_revision': AWS_REV, 'source_tag': AWS_VERSION, 'source_archive_sha256': ARCHIVES['aws-source.tar.gz'][1], 'archive_url': ARCHIVES['awscli.zip'][0], 'archive_sha256': ARCHIVES['awscli.zip'][1], 'binary_sha256': digest(aws_bin), 'binary_version': run([str(aws_bin), '--version']).strip(), 'service_model': 'bundled S3 2006-03-01', 'help_format': 'binary-generated roff captured with pass-through groff; no rendering changes', 'customizations': 'matching source tag; reference only'},
        'notices': 'apps/ex_storage_service_cli/priv/compatibility/NOTICE.txt', 'artifact_sha256': artifacts,
    }
    write_json(ROOT / 'docs/cli/compatibility/reference-lock.json', lock)
    print('Captured', len(tree['commands']), 'mc command nodes and', len(model['operations']), 'AWS S3 operations')


if __name__ == '__main__':
    main()
