from dag_doctor import __version__


def test_version_is_semver():
    major, minor, patch = __version__.split(".")
    assert all(part.isdigit() for part in (major, minor, patch))
