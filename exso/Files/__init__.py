from importlib.resources import files as resource_files
import tempfile, shutil
from pathlib import Path
import tzlocal
def __get_named_temp_dir(name):
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_path = Path(temp_dir.name)
    new_temp_dir_path = temp_dir_path.with_name(name)
    if new_temp_dir_path.exists(): # dont move, because if exists, shutil.move, actually puts it as a new dir inside the existing one...
        pass
    else:
        shutil.move(temp_dir_path, new_temp_dir_path)

    return new_temp_dir_path

project_name = 'exso'
system_tz = str(tzlocal.get_localzone())
files_dir = resource_files('{}.Files'.format(project_name))

_exso_dir = __get_named_temp_dir('exso')
_logs_dir = _exso_dir / 'logs'
_logs_dir.mkdir(exist_ok = True)
root_log = _logs_dir / 'root.log'
latest_logs_dir = _logs_dir / 'latest_logs'
latest_logs_dir.mkdir(exist_ok = True)
