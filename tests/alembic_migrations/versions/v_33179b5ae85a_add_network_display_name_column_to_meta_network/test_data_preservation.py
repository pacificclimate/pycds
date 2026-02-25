"""Data preservation and sequence tests:
- Check that existing history data is preserved during migration
- Verify that the sequence is correctly set after migration
- Ensure subsequent insertions work correctly
"""

import logging
import pytest
from sqlalchemy import text, inspect

logger = logging.getLogger("tests")


@pytest.mark.update20
def test_history_data_preservation_and_sequence(
    alembic_engine, alembic_runner, schema_name
):
    """Test that history data is preserved and sequence works after migration."""

    # Start at the previous revision
    alembic_runner.migrate_up_to("8c05da87cb79")

    # Insert test data into meta_network
    with alembic_engine.begin() as conn:
        # Insert a network
        conn.execute(
            text(
                f"""
                INSERT INTO {schema_name}.meta_network 
                (network_name, description, publish) 
                VALUES ('Test Network', 'Test Description', true)
                """
            )
        )

        # Get the network_id
        result = conn.execute(
            text(
                f"""
                SELECT network_id FROM {schema_name}.meta_network 
                WHERE network_name = 'Test Network'
                """
            )
        )
        network_id = result.scalar()

        # Update the network to create history entries
        conn.execute(
            text(
                f"""
                UPDATE {schema_name}.meta_network 
                SET description = 'Updated Description'
                WHERE network_id = {network_id}
                """
            )
        )

        conn.execute(
            text(
                f"""
                UPDATE {schema_name}.meta_network 
                SET description = 'Final Description'
                WHERE network_id = {network_id}
                """
            )
        )

        # Count history records before migration
        result = conn.execute(
            text(
                f"""
                SELECT COUNT(*) FROM {schema_name}.meta_network_hx
                WHERE network_id = {network_id}
                """
            )
        )
        history_count_before = result.scalar()

        # Get the max history ID before migration
        result = conn.execute(
            text(
                f"""
                SELECT MAX(meta_network_hx_id) FROM {schema_name}.meta_network_hx
                """
            )
        )
        max_hx_id_before = result.scalar()

    # Run the migration
    alembic_runner.migrate_up_to("33179b5ae85a")

    with alembic_engine.begin() as conn:
        # Verify history records still exist
        result = conn.execute(
            text(
                f"""
                SELECT COUNT(*) FROM {schema_name}.meta_network_hx
                WHERE network_id = {network_id}
                """
            )
        )
        history_count_after = result.scalar()
        assert history_count_after == history_count_before, (
            f"History count changed: before={history_count_before}, "
            f"after={history_count_after}"
        )

        # Verify network_display_name was generated for history records
        result = conn.execute(
            text(
                f"""
                SELECT COUNT(*) FROM {schema_name}.meta_network_hx
                WHERE network_id = {network_id} AND network_display_name IS NOT NULL
                """
            )
        )
        history_with_key_count = result.scalar()
        assert (
            history_with_key_count == history_count_after
        ), "Not all history records have network_display_name populated"

        # Verify the network_display_name matches expected format
        result = conn.execute(
            text(
                f"""
                SELECT network_display_name FROM {schema_name}.meta_network_hx
                WHERE network_id = {network_id}
                LIMIT 1
                """
            )
        )
        network_display_name = result.scalar()
        assert (
            network_display_name == "Test Network"
        ), f"Generated network_display_name '{network_display_name}' doesn't match expected 'Test Network'"

        # Verify the sequence is set correctly by inserting a new network
        # This should trigger the history tracking and use the next sequence value
        conn.execute(
            text(
                f"""
                INSERT INTO {schema_name}.meta_network 
                (network_name, description, publish) 
                VALUES ('New Test Network', 'New Description', true)
                """
            )
        )

        # Get the new network_id
        result = conn.execute(
            text(
                f"""
                SELECT network_id FROM {schema_name}.meta_network 
                WHERE network_name = 'New Test Network'
                """
            )
        )
        new_network_id = result.scalar()

        # Update it to create a history entry - this will test the sequence
        conn.execute(
            text(
                f"""
                UPDATE {schema_name}.meta_network 
                SET description = 'Updated New Description'
                WHERE network_id = {new_network_id}
                """
            )
        )

        # Verify the new history record was created successfully
        result = conn.execute(
            text(
                f"""
                SELECT meta_network_hx_id FROM {schema_name}.meta_network_hx
                WHERE network_id = {new_network_id}
                ORDER BY meta_network_hx_id
                """
            )
        )
        new_hx_ids = [row[0] for row in result.fetchall()]

        assert len(new_hx_ids) > 0, "No history records created for new network"

        # Verify the new history IDs are greater than the old max
        for hx_id in new_hx_ids:
            assert (
                hx_id > max_hx_id_before
            ), f"New history ID {hx_id} is not greater than old max {max_hx_id_before}"

        # Verify network_display_name was auto-generated for the new network
        result = conn.execute(
            text(
                f"""
                SELECT network_display_name FROM {schema_name}.meta_network
                WHERE network_id = {new_network_id}
                """
            )
        )
        new_network_display_name = result.scalar()
        assert (
            new_network_display_name == "New Test Network"
        ), f"Generated network_display_name '{new_network_display_name}' doesn't match expected 'New Test Network'"

        # Verify network_display_name in history matches
        result = conn.execute(
            text(
                f"""
                SELECT DISTINCT network_display_name FROM {schema_name}.meta_network_hx
                WHERE network_id = {new_network_id}
                """
            )
        )
        new_hx_network_display_name = result.scalar()
        assert (
            new_hx_network_display_name == "New Test Network"
        ), f"History network_display_name '{new_hx_network_display_name}' doesn't match expected 'New Test Network'"


@pytest.mark.update20
def test_sequence_continuity_with_no_gaps(alembic_engine, alembic_runner, schema_name):
    """Test that the sequence has no gaps after migration."""

    # Start at the previous revision
    alembic_runner.migrate_up_to("8c05da87cb79")

    with alembic_engine.begin() as conn:
        # Get current max ID before migration
        result = conn.execute(
            text(
                f"""
                SELECT COALESCE(MAX(meta_network_hx_id), 0) 
                FROM {schema_name}.meta_network_hx
                """
            )
        )
        max_id_before = result.scalar()

    # Run the migration
    alembic_runner.migrate_up_to("33179b5ae85a")

    with alembic_engine.begin() as conn:
        # Check the sequence value
        result = conn.execute(
            text(
                f"""
                SELECT last_value, is_called 
                FROM {schema_name}.meta_network_hx_meta_network_hx_id_seq
                """
            )
        )
        row = result.fetchone()
        last_value, is_called = row[0], row[1]

        # If is_called is true, the last value has been used and next will be last_value + 1
        # The migration uses COALESCE(MAX(...), 1) so minimum is 1 even when table is empty
        expected_last_value = max(max_id_before, 1)

        assert (
            is_called == True
        ), "Sequence should be marked as called after setval with true"
        assert last_value == expected_last_value, (
            f"Sequence last_value {last_value} doesn't match expected {expected_last_value} "
            f"(max_id_before was {max_id_before})"
        )
