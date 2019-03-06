import collections
import io
import os
from pathlib import Path
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class SuitcaseUtilsError(Exception):
    ...


class SuitcaseUtilsValueError(SuitcaseUtilsError):
    ...


class SuitcaseUtilsTypeError(SuitcaseUtilsError):
    ...


class ModeError(SuitcaseUtilsError):
    ...


class UnknownEventType(SuitcaseUtilsError):
    ...


class MultiFileManager:
    """
    A class that manages multiple files.

    Parameters
    ----------
    directory : str or Path
        The directory (as a string or as a Path) to create teh files inside.
    allowed_modes : Iterable
        Modes accepted by ``MultiFileManager.open``. By default this is
        retricted to "exclusive creation" modes ('x', 'xt', 'xb') which raise
        an error if the file already exists. This choice of defaults is meant
        to protect the user for unintentionally overwriting old files. In
        situations where overwrite ('w', 'wb') or append ('a', 'r+b') are
        needed, they can be added here.

    This design is inspired by Python's zipfile and tarfile libraries.
    """
    def __init__(self, directory, allowed_modes=('x', 'xt', 'xb')):
        self._directory = Path(directory)
        self._reserved_names = set()
        self._artifacts = collections.defaultdict(list)
        self._files = []
        self._allowed_modes = set(allowed_modes)

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
            A label for the sort of content being stored, such as
            'stream_data' or 'metadata'.
        postfix : string
            Postfix for the file name. Must be unique for this Manager.

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
        ----------
        label : string
            A label for the sort of content being stored, such as
            'stream_data' or 'metadata'.
        postfix : string
            Postfix for the file name. Must be unique for this Manager.
        mode : string
            One of the ``allowed_modes`` set in __init__``. Default set of
            options is ``{'x', 'xt', xb'}`` --- 'x' or 'xt' for text, 'xb' for
            binary.
        encoding : string or None
            Passed through open. See Python open documentation for allowed
            values. Only applicable to text mode.
        errors : string or None
            Passed through to open. See Python open documentation for allowed
            values.

        Returns
        -------
        file : handle
        """
        if mode not in self._allowed_modes:
            raise ModeError(
                f'The mode passed to MultiFileManager.open is {mode} but needs '
                f'to be one of {self._allowed_modes}')
        filepath = self.reserve_name(label, postfix)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        f = open(filepath, mode=mode, encoding=encoding, errors=errors)
        self._files.append(f)
        return f

    def close(self):
        '''close all files opened by the manager
        '''
        for f in self._files:
            f.close()


class PersistentStringIO(io.StringIO):
    ''' A StringIO that does not clear the buffer when closed.

        .. note::

            This StringIO subclass behaves like StringIO except that its
            close() method, which would normally clear the buffer, has no
            effect. The clear() method, however, may still be used.
    '''
    def close(self):
        # Avoid clearing the buffer before caller of ``export`` can access it.
        pass


class PersistentBytesIO(io.BytesIO):
    ''' A BytesIO that does not clear the buffer when closed.

        .. note::

            This BytesIO subclass behaves like BytesIO except that its
            close() method, which would normally clear the buffer, has no
            effect. The clear() method, however, may still be used.
    '''
    def close(self):
        # Avoid clearing the buffer before caller of ``export`` can access it.
        pass


class MemoryBuffersManager:
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
        This action is not valid on this manager. It will always raise.

        Parameters
        ----------
        label : string
            A label for the sort of content being stored, such as
            'stream_data' or 'metadata'.
        postfix : string
            Relative file path. Must be unique for this Manager.

        Raises
        ------
        SuitcaseUtilsTypeError
        """
        raise SuitcaseUtilsTypeError(
            "MemoryBuffersManager is incompatible with exporters that require "
            "explicit filenames.")

    def open(self, label, postfix, mode, encoding=None, errors=None):
        """
        Request a file handle.

        Like the built-in open function, this may be used as a context manager.

        Parameters
        ----------
        label : string
            A label for the sort of content being stored, such as
            'stream_data' or 'metadata'.
        postfix : string
            Relative file path (simply used as an identifer in this case, as
            there is no actual file). Must be unique for this Manager.
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
                f"The postfix {postfix} must be structured like a relative "
                f"file path.")
        name = Path(postfix).expanduser().resolve()
        if name in self._reserved_names:
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix!r} has already been used.")
        self._reserved_names.add(name)
        if mode in ('x', 'xt'):
            buffer = PersistentStringIO()
        elif mode == 'xb':
            buffer = PersistentBytesIO()
        else:
            raise ModeError(
                f"The mode passed to MemoryBuffersManager.open is {mode} but "
                f"needs to be one of 'x', 'xt' or 'xb'.")
        self._artifacts[label].append(buffer)
        self.buffers[postfix] = buffer
        return buffer

    def close(self):
        '''Close all buffers opened by the manager.
        '''
        for f in self.buffers.values():
            f.close()
