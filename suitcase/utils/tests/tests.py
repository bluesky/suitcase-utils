import pytest

from suitcase.utils import (
    MultiFileManager, MemoryBuffersManager, SuitcaseUtilsValueError,
    SuitcaseUtilsTypeError, ModeError)
pytest


def test_multifile_basic_operation(tmp_path):
    manager = MultiFileManager(tmp_path)
    f = manager.open('thing', 'stuff', 'x')
    name1 = tmp_path / 'stuff'
    name2 = manager.reserve_name('thing', 'stuff2')

    # Check that name clashes are not allowed.
    with pytest.raises(SuitcaseUtilsValueError):
        manager.open('thing', 'stuff', 'x')
    with pytest.raises(SuitcaseUtilsValueError):
        manager.reserve_name('thing', 'stuff')

    # Check that abs paths are not allowed.
    with pytest.raises(SuitcaseUtilsValueError):
        manager.open('thing', '/stuff', 'x')

    # Check that only certain modes are allowed.
    for mode in ('x', 'xt', 'xb'):
        manager.open('mode_tests', f'stuff-{mode}', mode)
    for mode in ('w', 'wt', 'wb'):
        with pytest.raises(ModeError):
            manager.open('mode_tests', f'stuff-{mode}', mode)

    f.write('test')
    assert not f.closed
    manager.close()
    assert f.closed
    with open(name1) as f:
        actual = f.read()
    assert actual == 'test'
    assert [name1, name2] == manager.artifacts['thing']


def test_memory_buffers_basic_operation():
    manager = MemoryBuffersManager()
    f = manager.open('thing', 'stuff', 'x')

    # Check that reserve_name fails explicitly on MemoryBuffersManager.
    with pytest.raises(SuitcaseUtilsTypeError):
        manager.reserve_name('thing', 'stuff')

    # Check that name clashes are not allowed.
    with pytest.raises(SuitcaseUtilsValueError):
        manager.open('thing', 'stuff', 'x')

    # Check that abs paths are not allowed.
    with pytest.raises(SuitcaseUtilsValueError):
        manager.open('thing', '/stuff', 'x')

    # Check that only certain modes are allowed.
    for mode in ('x', 'xt', 'xb'):
        manager.open('mode_tests', f'stuff-{mode}', mode)
    for mode in ('w', 'wt', 'wb'):
        with pytest.raises(ModeError):
            manager.open('mode_tests', f'stuff-{mode}', mode)

    f.write('test')
    assert not f.closed
    manager.close()
    assert not f.closed  # Close is a no-op on Persistent{String|Bytes}IO.
    f.seek(0)
    actual = f.read()
    assert actual == 'test'
    assert [f] == manager.artifacts['thing']


def test_fixture(example_data):
    "Just exercise the example_data test fixture."
    assert type(example_data()) is list


def test_fixture_with_ignore(example_data):
    "Exercise the example_data 'ignore' argument that skip some parameters."
    assert type(example_data(ignore=['bulk_events'])) is list
