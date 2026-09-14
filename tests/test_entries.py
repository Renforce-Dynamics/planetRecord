from pathlib import Path
import socket

import pytest

from planetr.cli import main


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('action,entry', [
    ('record', 'entry_recorder.yaml'), ('legacy', 'entry_legacy_recorder.yaml'),
])
def test_each_service_uses_its_own_explicit_entry_without_io(action, entry, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(socket, 'socket', lambda *a, **k: pytest.fail('offline check opened a socket'))
    assert main([action, '--config', str(ROOT / 'configs/entry' / entry), '--check']) == 0
    with pytest.raises(SystemExit) as error:
        main([action, '--check'])
    assert error.value.code == 2


def test_missing_entry_does_not_load_an_installed_profile(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises((OSError, ValueError)):
        main(['record', '--config', 'configs/entry/entry_recorder.yaml', '--check'])
    assert not list((ROOT / 'src').rglob('*.yaml'))
