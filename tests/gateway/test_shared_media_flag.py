"""Startup flag is literal-only and never enables uploads by default."""
import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize('value,expected', [(None, 'False'), ('', 'False'), ('true', 'False'), ('0', 'False'), (' 1', 'False'), ('1', 'True')])
def test_shared_media_startup_opt_in(value, expected, tmp_path):
    env = {'PATH': os.environ.get('PATH', ''), 'HOME': str(tmp_path), 'HERMES_HOME': str(tmp_path),
           'PYTHONPATH': os.getcwd(), 'PYTHONDONTWRITEBYTECODE': '1'}
    if value is not None:
        env['HERMES_SHARED_THREAD_MEDIA_V1'] = value
    result = subprocess.run([sys.executable, '-c',
        'from gateway.session_automation import SHARED_THREAD_MEDIA_V1; print(SHARED_THREAD_MEDIA_V1)'],
        env=env, check=True, capture_output=True, text=True, timeout=30)
    assert result.stdout.strip() == expected
