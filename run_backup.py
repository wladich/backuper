#!/usr/bin/env python2
# coding: utf-8
import sys
import os
import logging
import subprocess
from threading import Timer
import json
import shutil
import posixpath
import time
import urllib
import calendar
import tempfile
import json
import traceback

import yaml


class TimeoutError(Exception):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return "Executing command '%s' hit timout" % self.cmd


class LocalStorageBackend(object):
    def __init__(self, root):
        self.root_dir = root

    def put_file(self, src_file_path, dest_filename):
        shutil.copy(src_file_path, os.path.join(self.root_dir, dest_filename))

    def list_files(self):
        return os.listdir(self.root_dir)

    def delete_file(self, filename):
        os.remove(os.path.join(self.root_dir, filename))

    def get_file(self, src_filename, dest_file_path):
        shutil.copy(os.path.join(self.root_dir, src_filename), dest_file_path)


class WebdavStorageBackend(object):
    def __init__(self, root, host, login, password):
        import easywebdav
        self.root_dir = root
        protocol, _, host = host.partition('://')
        self.client = easywebdav.connect(host, username=login, password=password, protocol=protocol)

    def put_file(self, src_file_path, dest_filename):
        dest_path = posixpath.join(self.root_dir, dest_filename)
        self.client.upload(src_file_path, dest_path)

    def list_files(self):
        return [urllib.unquote(posixpath.basename(f.name)) for f in self.client.ls(self.root_dir)]

    def delete_file(self, filename):
        path = posixpath.join(self.root_dir, filename)
        self.client.delete(path)

    def get_file(self, src_filename, dest_file_path):
        src_file_path = posixpath.join(self.root_dir, src_filename)
        tmp_path = dest_file_path + '.tmp'
        self.client.download(src_file_path, tmp_path)
        os.rename(tmp_path, dest_file_path)


class ResticStorageBackend(object):
    tag_prefix = 'backup_filename='
    restic_snapshot_filename = ''

    def __init__(self, repo, password, cache_dir, tmp_dir, rclone_config_file='/etc/rclone.conf'):
        self.restic_repo = repo
        self.restic_password = password
        self.cache_dir = cache_dir
        self.tmp_dir = tmp_dir
        self.rclone_config_file = rclone_config_file
        try:
            self.run_restic('snapshots')
        except:
            self.run_restic('init')

    def get_restic_command(self, command, args):
        return ['restic', '-r', self.restic_repo,
               '-o',
               'rclone.program=/usr/bin/rclone --retries=10 --retries-sleep=3s --config=%s' % self.rclone_config_file,
               '--cleanup-cache',
               '--cache-dir', self.cache_dir,
               command] + list(args)

    def run_restic(self, command, args=None, stdin=None, stdout=subprocess.PIPE):
        env = {
            'RESTIC_PASSWORD': self.restic_password,
            'TMPDIR': self.tmp_dir,
        }
        cmd = self.get_restic_command(command, args or [])
        p = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.PIPE, stdin=stdin, env=env)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise Exception('Command %s returned exit status %s.\nSTDOUT:\n%s\nSTDERR:\n%s\n' % (
                cmd, p.returncode, stdout, stderr))
        return stdout

    def put_file(self, src_file_path, dest_filename):
        with open(src_file_path, 'rb') as f:
            self.run_restic('backup', ['--stdin', '--tag', self.tag_prefix + dest_filename], stdin=f)

    def list_files(self):
        names = []
        for snapshot in json.loads(self.run_restic('snapshots', ['--json'])):
            for tag in snapshot['tags']:
                if tag.startswith(self.tag_prefix):
                    names.append(tag[len(self.tag_prefix):])
                    break
            else:
                raise Exception('Filename tag not found for snapshot %s' % snapshot['short_id'])
        return names

    def delete_file(self, filename):
        raise NotImplementedError

    def get_file(self, src_filename, dest_file_path):
        with open(dest_file_path, 'wb') as f:
            self.run_restic('dump', ['--tag', self.tag_prefix + src_filename, 'latest', 'stdin'], stdout=f)


class RcloneStorageBackend(object):
    def __init__(self, root, config_file, backend_name, cleanup_on_delete=False):
        self.backend_name = backend_name
        if not root.endswith('/'):
            root += '/'
        self.root_dir = root
        self.config_file = config_file
        self.cleanup_on_delete = cleanup_on_delete

    def _remote_specifier(self, filename=None):
        path = self.root_dir
        if filename is not None:
            path += filename
        return '%s:%s' % (self.backend_name, path)

    def _run_command(self, args):
        cmd = ['rclone', '--config', self.config_file] + args
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise Exception('Command %s returned exit status %s.\nSTDOUT:\n%s\nSTDERR:\n%s\n' % (
                cmd, p.returncode, stdout, stderr))
        return stdout

    def put_file(self, src_file_path, dest_filename):
        temp_dir = tempfile.mkdtemp()
        try:
            shutil.copy(src_file_path, os.path.join(temp_dir, dest_filename))
            self._run_command(['copy', os.path.join(temp_dir, dest_filename),
                               self._remote_specifier()])
        finally:
            shutil.rmtree(temp_dir)

    def list_files(self):
        s = self._run_command(['lsjson', self._remote_specifier()])
        return [item['Name'] for item in json.loads(s)]

    def delete_file(self, filename):
        self._run_command(['deletefile', self._remote_specifier(filename)])
        if self.cleanup_on_delete:
            self._run_command(['cleanup', self._remote_specifier()])

    def get_file(self, src_filename, dest_file_path):
        temp_dir = tempfile.mkdtemp()
        try:
            self._run_command(['copy', self._remote_specifier(src_filename), temp_dir])
            shutil.move(os.path.join(temp_dir, src_filename), dest_file_path)
        finally:
            shutil.rmtree(temp_dir)


storage_classes = {
    'local': LocalStorageBackend,
    'webdav': WebdavStorageBackend,
    'rclone': RcloneStorageBackend,
    'restic': ResticStorageBackend,
}


class BackupApp(object):
    datetime_format = '%Y-%m-%d_%H:%M:%S'

    def __init__(self, config_filename):
        self.config = config = yaml.safe_load(open(config_filename))
        self._logger = self.get_logger(config['log_file'], logging.DEBUG)

    def make_backup_filename(self):
        date_str = time.strftime(self.datetime_format, time.gmtime())
        return '%s%s%s' % (self.config['prefix'], date_str, self.config['suffix'])

    def get_ts_from_backup_name(self, s):
        date_fmt = '%s%s%s' % (self.config['prefix'], self.datetime_format, self.config['suffix'])
        try:
            return calendar.timegm(time.strptime(s, date_fmt))
        except ValueError:
            return None

    def get_storage(self, storage_config):
        storage_config = storage_config.copy()
        storage_config.pop('retention', None)
        storage_class = storage_classes[storage_config.pop('type')]
        storage = storage_class(**storage_config)
        return storage

    def upload_backup(self):
        dest_filename = self.make_backup_filename()
        errors = []
        for storage_name, storage_config in self.config['storages'].iteritems():
            storage = self.get_storage(storage_config)
            self.log('INFO', 'uploading', storage=storage_name, filename=dest_filename)
            try:
                storage.put_file(src_file_path=self.config['backup_file'], dest_filename=dest_filename)
            except Exception:
                errors.append(''.join(traceback.format_exc()))
        if errors:
            delim = '\n' + '-' * 80 + '\n'
            raise Exception(delim.join(errors))
        return dest_filename

    def verify_backup(self, filename):
        for storage_name, storage_config in self.config['storages'].iteritems():
            storage = self.get_storage(storage_config)
            local_file_path = self.config['backup_file']
            self.cleanup()
            self.log('INFO', 'Downloading file', storage=storage_name, filename=filename)
            storage.get_file(filename, local_file_path)
            try:
                self.execute_script(self.config['verify'], self.config['verify_timeout'])
            except (TimeoutError, subprocess.CalledProcessError):
                self.log('EXCEPTION', 'Verification failed', storage=storage_name, filename=filename)
                return
        with open(self.config['success_timestamp_file'], 'w') as f:
            f.write(str(int(time.time())) + '\n')

    def get_outdated_backup_dates(self, file_ts, retention_config):
        if not retention_config:
            return []
        outdated = []
        now = time.time()
        for period in retention_config:
            period['end'] = now - period['older_days'] * 24 * 3600
            if 'interval_hours' in period:
                period['interval'] = period['interval_hours'] * 3600
        retention_periods = iter(sorted(retention_config, key=lambda rec: rec['end']))
        current_period = None

        for (filename, ts) in sorted(file_ts, key=lambda fd: fd[1]):
            while current_period is None or ts > current_period['end']:
                try:
                    current_period = next(retention_periods)
                except StopIteration:
                    return outdated
                current_interval_n = None
            if not current_period.get('store', True):
                outdated.append((filename, ts))
                continue
            interval_n = int(ts / current_period['interval'])
            if interval_n == current_interval_n:
                outdated.append((filename, ts))
            else:
                current_interval_n = interval_n
        return outdated

    def delete_old_backups(self):
        for storage_name, storage_config in self.config['storages'].iteritems():
            storage = self.get_storage(storage_config)
            filenames = storage.list_files()
            file_dates = []
            for filename in filenames:
                file_ts = self.get_ts_from_backup_name(filename)
                if file_ts is not None:
                    file_dates.append((filename, file_ts))
            retention_config = storage_config.get('retention') or self.config.get('retention')

            for (filename, _) in self.get_outdated_backup_dates(file_dates, retention_config):
                self.log('INFO', 'Remove old file', storage=storage_name, filename=filename)
                storage.delete_file(filename)

    def cleanup(self):
        path = self.config['backup_file']
        if os.path.exists(path):
            self.log('DEBUG', 'Cleanup', filename=path)
            os.unlink(self.config['backup_file'])

    def run(self):
        try:
            self.log('INFO', 'Started')
            self.execute_script(self.config['prepare_backup'], self.config['prepare_backup_timeout'])
            filename = self.upload_backup()
            self.delete_old_backups()
            self.verify_backup(filename)
            self.cleanup()
            self.log('INFO', 'Ended')
        except:
            self.log('EXCEPTION')
            raise

    def get_logger(self, filename, level):
        log = logging.getLogger(__name__)
        log.setLevel(level)
        if not filename:
            log_handler = logging.StreamHandler()
        else:
            log_handler = logging.FileHandler(filename)
        log_handler.setLevel(level)
        log_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        log_handler.setFormatter(log_formatter)
        log.addHandler(log_handler)
        return log

    def log(self, level, message='', **extra):
        if extra:
            message += ' ' + json.dumps(extra)
        if level == 'EXCEPTION':
            self._logger.exception(message)
        else:
            self._logger.log(getattr(logging, level), message)

    def execute_script(self, script, timeout):
        self.log('DEBUG', 'Executing script %r' % script)
        p = subprocess.Popen(script, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
        hit_timeout = {'value': False}

        def on_timeout():
            p.kill()
            hit_timeout['value'] = True

        timer = Timer(timeout, on_timeout, [])
        try:
            timer.start()
            stdout, stderr = p.communicate()
        finally:
            timer.cancel()
        if hit_timeout['value']:
            self.log('ERROR', 'Script timeout', return_cod=p.returncode, stdout=stdout, stderr=stderr)
            raise TimeoutError(script)
        self.log('DEBUG', 'Script result', return_cod=p.returncode, stdout=stdout, stderr=stderr)
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, script)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Usage: %s CONFIG_FILE' % os.path.basename(__file__)
        exit(1)
    BackupApp(sys.argv[1]).run()
