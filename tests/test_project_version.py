import toml
import arrow_pd_parser


def test_pyproject_toml_matches_version():
    with open("pyproject.toml") as f:
        proj = toml.load(f)
    assert arrow_pd_parser.__version__ == proj["tool"]["poetry"]["version"]
