from unittest.mock import Mock

import pytest

from fsspec.transaction import FileActor


@pytest.mark.parametrize("commit", [True, False])
def test_actor_cleans_deferred_files_after_failure(commit):
    actor = FileActor()
    first, failed, pending = Mock(), Mock(), Mock()
    for file in (first, failed, pending):
        actor.append(file)

    operation = "commit" if commit else "discard"
    failure = OSError("original transaction error")
    getattr(failed, operation).side_effect = failure
    pending.discard.side_effect = RuntimeError("cleanup error")

    with pytest.raises(OSError, match="original transaction error") as raised:
        getattr(actor, operation)()
    assert raised.value is failure

    getattr(first, operation).assert_called_once_with()
    if commit:
        first.discard.assert_not_called()
        failed.discard.assert_called_once_with()
        pending.commit.assert_not_called()
    else:
        assert failed.discard.call_count == 2
    pending.discard.assert_called_once_with()

    for file in (first, failed, pending):
        file.reset_mock()
    actor.commit()
    actor.discard()
    for file in (first, failed, pending):
        assert file.mock_calls == []
