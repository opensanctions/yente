from yente.data.dataset import Dataset


def test_dataset_has_version():
    # Given a dataset
    ds = Dataset()
    # It can get its available versions
    # It caches its available versions
    # And can be asked to refresh the available versions
    # And it can get the newest available version
