package io.r2dbc.postgresql.api;

import io.r2dbc.postgresql.message.backend.Field;

import java.util.Map;

/**
 * Postgres notice.
 */
public class Notice {

    /**
     * Notice messages by {@link Field.FieldType}.
     */
    public final Map<Field.FieldType, String> fields;

    /**
     * @param fields notice messages by {@link Field.FieldType}
     */
    public Notice(Map<Field.FieldType, String> fields) {
        this.fields = fields;
    }
}
