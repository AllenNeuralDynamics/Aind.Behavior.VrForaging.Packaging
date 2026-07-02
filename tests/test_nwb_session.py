from unittest.mock import MagicMock, patch


def test_nwb_session_uses_local_schema_when_flag_set(tmp_path):
    """NwbSession(root_path, use_local_schema=True) calls from_root_path, not from_doc_db."""
    from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession, _AindDataSchemaJson

    mock_schema = MagicMock(spec=_AindDataSchemaJson)
    mock_schema.data_description = MagicMock()
    mock_schema.data_description.name = "test_session"
    mock_dataset = MagicMock()

    with (
        patch.object(_AindDataSchemaJson, "from_root_path", return_value=mock_schema) as mock_local,
        patch.object(_AindDataSchemaJson, "from_doc_db") as mock_db,
        patch(
            "aind_behavior_vr_foraging_packaging.nwb_file.aind_behavior_vr_foraging.data_contract.dataset",
            return_value=mock_dataset,
        ),
    ):
        NwbSession(tmp_path, use_local_schema=True)
        mock_local.assert_called_once_with(tmp_path)
        mock_db.assert_not_called()


def test_nwb_session_uses_doc_db_by_default(tmp_path):
    """NwbSession(root_path) calls from_doc_db, not from_root_path."""
    from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession, _AindDataSchemaJson

    mock_schema = MagicMock(spec=_AindDataSchemaJson)
    mock_schema.data_description = MagicMock()
    mock_schema.data_description.name = "test_session"
    mock_dataset = MagicMock()

    with (
        patch.object(_AindDataSchemaJson, "from_doc_db", return_value=mock_schema) as mock_db,
        patch.object(_AindDataSchemaJson, "from_root_path") as mock_local,
        patch(
            "aind_behavior_vr_foraging_packaging.nwb_file.aind_behavior_vr_foraging.data_contract.dataset",
            return_value=mock_dataset,
        ),
    ):
        NwbSession(tmp_path)
        mock_db.assert_called_once()
        mock_local.assert_not_called()
