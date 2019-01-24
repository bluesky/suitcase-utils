from pathlib import Path
import collections
import os
import io
import event_model
from bluesky.plans import count
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class SuitcaseUtilsError(Exception):
    ...


class SuitcaseUtilsValueError(SuitcaseUtilsError):
    ...


class SuitcaseUtilsTypeError(SuitcaseUtilsError):
    ...


class SuitcaseUtilsModeError(SuitcaseUtilsError):
    ...


class SuitcaseUtilsUnknownEventType(SuitcaseUtilsError):
    ...


class MultiFileWrapper:
    """
    A class that manages multiple files.

    This design is inspired by Python's zipfile and tarfile libraries.
    """
    def __init__(self, directory):
        self._directory = Path(directory)
        self._reserved_names = set()
        self._artifacts = collections.defaultdict(list)

    @property
    def artifacts(self):
        return dict(self._artifacts)

    def reserve_name(self, label, postfix):
        """
        Ask the wrapper for a filepath.

        An external library that needs a filepath (not a handle)
        may use this instead of the ``open`` method.

        Parameters
        ----------
        label : string
            partial file name (i.e. stream name)
        postfix : string
            relative file path and filename

        Returns
        -------
        name : Path
         """
        if Path(postfix).is_absolute():
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix!r} must be structured like a relative "
                f"file path.")
        name = (self._directory / Path(postfix)).expanduser().resolve()
        if name in self._reserved_names:
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix!r} has already been used.")
        self._reserved_names.add(name)
        self._artifacts[label].append(name)
        return name

    def open(self, label, postfix, mode, encoding=None, errors=None):
        """
        Request a file handle.

        Like the built-in open function, this may be used as a context manager.

        Parameters
        -------
        label : string
            partial file name (i.e. stream name)
        postfix : string
            postfix for the filenames.
        mode : {'x', 'xt', xb'}
            'x' or 'xt' for text, 'xb' for binary
        encoding : string or None
            Passed through open.  See Python open documentation for allowed
            values. Only applicable to text mode.
        errors : string or None
            Passed through to open. See Python open documentation for allowed
            values.
       Returns
       -------
       file : handle
        """
        filepath = self.reserve_name(label, postfix)
        # create the directories if they don't yet exist
        if not os.path.exists(os.path.dirname(filepath)):
            try:
                os.makedirs(os.path.dirname(filepath))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        if mode not in ['x', 'xt', 'xb']:
            raise SuitcaseUtilsModeError(
                f'the mode passed to MultiFileWrapper.open is {mode} but needs'
                ' to be one of "x", "xt" or "xb"')
        return open(filepath, mode=mode, encoding=encoding, errors=errors)


class PersistentStringIO(io.StringIO):
    ''' A version of StringIO that avoids closing the file n a context manager.

    '''
    def __exit__(*except_detail):
        pass  # this avoids closing the file handle too early.


class PersistentBytesIO(io.BytesIO):
    ''' A version of BytesIO that avoids closing the file n a context manager.

    '''
    def __exit__(*except_detail):
        pass  # this avoids closing the file handle too early.


class MemoryBuffersWrapper:
    """
    A class that manages multiple StringIO and/or BytesIO instances.

    This design is inspired by Python's zipfile and tarfile libraries.

    This has a special buffers attribute which can be used to retrieve
    buffers created.
    """
    def __init__(self):
        self._reserved_names = set()
        self._artifacts = collections.defaultdict(list)
        self.buffers = {}  # maps postfixes to buffer objects

    @property
    def artifacts(self):
        return dict(self. _artifacts)

    def reserve_name(self, label, postfix):
        """
        Ask the wrapper for a filepath.

        An external library that needs a filepath (not a handle)
        may use this instead of the ``open`` method.

        Parameters
        -------
        label : string
            partial file name (i.e. stream name)
        postfix : string
            relative file path and filename

        Returns
        ----------
        filepath : Path
         """
        raise SuitcaseUtilsTypeError(
            "MemoryBuffersWrapper is incompatible with exporters that require "
            "explicit filenames.")

    def open(self, label, postfix, mode, encoding=None, errors=None):
        """
        Request a file handle.

        Like the built-in open function, this may be used as a context manager.

        Parameters
        -------
        label : string
            partial file name (i.e. stream name)
        postfix : string
            relative file path and filename
        mode : {'x', 'xt', xb'}
            'x' or 'xt' for text, 'xb' for binary
        encoding : string or None
            Not used. Accepted for compatibility with built-in open().
        errors : string or None
            Not used. Accepted for compatibility with built-in open().

       Returns
       -------
       file : handle
        """
        # Of course, in-memory buffers have no filepath, but we still expect
        # postfix to be a thing that looks like a relative filepath, and we use
        # it as a unique identifier for a given buffer.
        if Path(postfix).is_absolute():
            raise SuitcaseUtilsValueError(
                f"{postfix} must be structured like a relative file path.")
        name = Path(postfix).expanduser().resolve()
        if name in self._reserved_names:
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix!r} has already been used.")
        self._reserved_names.add(name)
        self._artifacts[label].append(name)
        if mode in ('x', 'xt'):
            buffer = PersistentStringIO()
        elif mode == 'xb':
            buffer = PersistentBytesIO()
        else:
            raise SuitcaseUtilsModeError(
                f'the mode passed to MemoryBuffersWrapper.open is {mode} but '
                'needs to be one of "x", "xt" or "xb"')
        self.buffers[postfix] = buffer
        return buffer


