import create_release


def test_calculate_new_version():
    new_version = create_release.calculate_new_version('3.10.0')
    assert new_version == '3.11.0'
