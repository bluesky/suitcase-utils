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

    Parameters:
    -----------
    directory : str or Path
        The directory (as a string or as a Path) to create teh files inside.

    This design is inspired by Python's zipfile and tarfile libraries.
    """
    def __init__(self, directory):
        self._directory = Path(directory)
        self._reserved_names = set()
        self._artifacts = collections.defaultdict(list)
        self._files = []

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
<<<<<<< HEAD
         """
=======
        """
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
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
<<<<<<< HEAD
=======

>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
        Returns
        -------
        file : handle
        """
        filepath = self.reserve_name(label, postfix)
        # create the directories if they don't yet exist
<<<<<<< HEAD
        if not os.path.exists(os.path.dirname(filepath)):
            try:
                os.makedirs(os.path.dirname(filepath))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        if mode not in ['x', 'xt', 'xb']:
            raise ModeError(
                f'the mode passed to MultiFileWrapper.open is {mode} but needs'
=======
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if mode not in ['x', 'xt', 'xb']:
            raise ModeError(
                f'the mode passed to MultiFileManager.open is {mode} but needs'
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
                ' to be one of "x", "xt" or "xb"')
        f = open(filepath, mode=mode, encoding=encoding, errors=errors)
        self._files.append(f)
        return f

    def close(self):
<<<<<<< HEAD
        '''close all files open by the manager
=======
        '''close all files opened by the manager
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
        '''
        for f in self._files:
            f.close()


class PersistentStringIO(io.StringIO):
<<<<<<< HEAD
    ''' A StringIO that does not clear the buffer when closed or excited from
    context.
=======
    ''' A StringIO that does not clear the buffer when closed.
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599

        .. note::

            This StringIO subclass behaves like StringIO except that its
            close() method, which would normally clear the buffer, has no
            effect. The clear() method, however, may still be used.
    '''
<<<<<<< HEAD
    def close():
        pass  # this avoids closing the file handle too early.


class PersistentBytesIO(io.BytesIO):
    ''' A BytesIO that does not clear the buffer when closed or exited from
    context.
=======
    def close(self):
        # Avoid clearing the buffer before caller of ``export`` can access it.
        pass


class PersistentBytesIO(io.BytesIO):
    ''' A BytesIO that does not clear the buffer when closed.
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599

        .. note::

            This BytesIO subclass behaves like BytesIO except that its
            close() method, which would normally clear the buffer, has no
            effect. The clear() method, however, may still be used.
    '''
<<<<<<< HEAD
    def close():
        pass  # this avoids closing the file handle too early.
=======
    def close(self):
        # Avoid clearing the buffer before caller of ``export`` can access it.
        pass
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599


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
<<<<<<< HEAD
         """
        raise SuitcaseUtilsTypeError(
            "MemoryBuffersWrapper is incompatible with exporters that require "
=======
        """
        raise SuitcaseUtilsTypeError(
            "MemoryBuffersManager is incompatible with exporters that require "
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
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

<<<<<<< HEAD
       Returns
       -------
       file : handle
=======
        Returns
        -------
        file : handle
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
        """
        # Of course, in-memory buffers have no filepath, but we still expect
        # postfix to be a thing that looks like a relative filepath, and we use
        # it as a unique identifier for a given buffer.
        if Path(postfix).is_absolute():
            raise SuitcaseUtilsValueError(
<<<<<<< HEAD
                f"{postfix} must be structured like a relative file path.")
=======
                f"The postfix {postfix} must be structured like a relative "
                f"file path.")
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
        name = Path(postfix).expanduser().resolve()
        if name in self._reserved_names:
            raise SuitcaseUtilsValueError(
                f"The postfix {postfix!r} has already been used.")
        self._reserved_names.add(name)
<<<<<<< HEAD
        self._artifacts[label].append(name)
=======
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
        if mode in ('x', 'xt'):
            buffer = PersistentStringIO()
        elif mode == 'xb':
            buffer = PersistentBytesIO()
        else:
            raise ModeError(
<<<<<<< HEAD
                f'the mode passed to MemoryBuffersWrapper.open is {mode} but '
                'needs to be one of "x", "xt" or "xb"')
=======
                f"The mode passed to MemoryBuffersManager.open is {mode} but "
                f"needs to be one of 'x', 'xt' or 'xb'.")
        self._artifacts[label].append(buffer)
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
        self.buffers[postfix] = buffer
        return buffer

    def close(self):
<<<<<<< HEAD
        '''close all files open by the manager
        '''
        for buffer in self.buffers.values():
            buffer.clear()
        # Note: .clear() is used not .close(), see Persistent*IO defn. above
=======
        '''Close all buffers opened by the manager.
        '''
        for f in self.buffers.values():
            f.close()
>>>>>>> ccb1ced23ad740f7c7c0e9a5a312643f125c1599
