package org.lsst.alerts;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration-style tests for schema evolution scenarios.
 * Specifically tests the case where:
 * - Iceberg table has schema 1102 (newer, with extra fields)
 * - Target Avro schema is 1100 (older, without those fields)
 * - Query must only select fields present in schema 1100
 */
public class TestAlertsToKafkaSchemaEvolution {

    private static Method buildQueryMethod;
    private static Field whereField;
    private static Field schemaExcludeField;
    private static Field limitField;
    private static Field orderByField;

    static {
        try {
            buildQueryMethod = AlertsToKafka.class.getDeclaredMethod(
                    "buildQuery", AlertsToKafka.Arguments.class, Schema.class);
            buildQueryMethod.setAccessible(true);

            whereField = AlertsToKafka.Arguments.class.getDeclaredField("where");
            whereField.setAccessible(true);
            schemaExcludeField = AlertsToKafka.Arguments.class.getDeclaredField("schemaExclude");
            schemaExcludeField.setAccessible(true);
            limitField = AlertsToKafka.Arguments.class.getDeclaredField("limit");
            limitField.setAccessible(true);
            orderByField = AlertsToKafka.Arguments.class.getDeclaredField("orderBy");
            orderByField.setAccessible(true);
        } catch (Exception e) {
            fail("Failed to set up reflection: " + e.getMessage());
        }
    }

    private static AlertsToKafka.Arguments createArguments(String where, Set<String> exclude, 
                                                          Integer limit, String orderBy) throws Exception {
        AlertsToKafka.Arguments args = new AlertsToKafka.Arguments();
        if (where != null) {
            whereField.set(args, where);
        }
        if (exclude != null) {
            schemaExcludeField.set(args, exclude);
        }
        if (limit != null) {
            limitField.set(args, limit);
        }
        if (orderBy != null) {
            orderByField.set(args, orderBy);
        }
        return args;
    }

    private static String buildQuery(AlertsToKafka.Arguments args, Schema schema) throws Exception {
        return (String) buildQueryMethod.invoke(null, args, schema);
    }

    /**
     * Creates a simplified version of schema 1100 (without exposureTime and mixedInstruments)
     */
    private static Schema createSchema1100() {
        // diaSource record (schema 1100 - WITHOUT exposureTime and mixedInstruments)
        List<Schema.Field> diaSourceFields = List.of(
                new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("visit", Schema.create(Schema.Type.LONG)),
                new Schema.Field("detector", Schema.create(Schema.Type.INT)),
                new Schema.Field("diaObjectId", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))),
                new Schema.Field("ssObjectId", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))),
                new Schema.Field("parentDiaSourceId", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))),
                new Schema.Field("midpointMjdTai", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("psfFlux", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)))),
                new Schema.Field("reliability", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)))),
                new Schema.Field("band", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("timeProcessedMjdTai", Schema.create(Schema.Type.DOUBLE))
        );
        Schema diaSourceSchema = Schema.createRecord("diaSource", null, "lsst.v11_0", false);
        diaSourceSchema.setFields(diaSourceFields);

        // diaObject record
        List<Schema.Field> diaObjectFields = List.of(
                new Schema.Field("diaObjectId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("nDiaSources", Schema.create(Schema.Type.INT))
        );
        Schema diaObjectSchema = Schema.createRecord("diaObject", null, "lsst.v11_0", false);
        diaObjectSchema.setFields(diaObjectFields);

        // diaForcedSource record
        List<Schema.Field> diaForcedSourceFields = List.of(
                new Schema.Field("diaForcedSourceId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("diaObjectId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("visit", Schema.create(Schema.Type.LONG)),
                new Schema.Field("detector", Schema.create(Schema.Type.INT))
        );
        Schema diaForcedSourceSchema = Schema.createRecord("diaForcedSource", null, "lsst.v11_0", false);
        diaForcedSourceSchema.setFields(diaForcedSourceFields);

        // Top-level alert record (schema 1100)
        List<Schema.Field> alertFields = List.of(
                new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("observation_reason", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("target_name", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("diaSource", diaSourceSchema),
                new Schema.Field("prvDiaForcedSources", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(diaForcedSourceSchema)))),
                new Schema.Field("diaObject", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), diaObjectSchema))),
                new Schema.Field("cutoutDifference", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)))),
                new Schema.Field("cutoutScience", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)))),
                new Schema.Field("cutoutTemplate", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES))))
        );
        Schema alertSchema = Schema.createRecord("alert", null, "lsst.v11_0", false);
        alertSchema.setFields(alertFields);

        return alertSchema;
    }

    /**
     * Creates a simplified version of schema 1102 (WITH exposureTime and mixedInstruments)
     */
    private static Schema createSchema1102() {
        // Start with schema 1100 diaSource fields plus new ones
        List<Schema.Field> diaSourceFields = List.of(
                new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("visit", Schema.create(Schema.Type.LONG)),
                new Schema.Field("detector", Schema.create(Schema.Type.INT)),
                new Schema.Field("diaObjectId", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))),
                new Schema.Field("ssObjectId", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))),
                new Schema.Field("parentDiaSourceId", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))),
                new Schema.Field("midpointMjdTai", Schema.create(Schema.Type.DOUBLE)),
                // NEW FIELDS IN SCHEMA 1102
                new Schema.Field("exposureTime", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)))),
                new Schema.Field("templateMjdMean", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)))),
                new Schema.Field("templateMjdMin", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)))),
                new Schema.Field("templateMjdMax", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)))),
                new Schema.Field("templateInstrument", Schema.create(Schema.Type.STRING)),
                new Schema.Field("templateVersion", Schema.create(Schema.Type.INT)),
                new Schema.Field("mixedInstruments", Schema.create(Schema.Type.BOOLEAN)),
                // End of new fields
                new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("psfFlux", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)))),
                new Schema.Field("reliability", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)))),
                new Schema.Field("reliabilityVersion", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("band", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("timeProcessedMjdTai", Schema.create(Schema.Type.DOUBLE))
        );
        Schema diaSourceSchema = Schema.createRecord("diaSource", null, "lsst.v11_2", false);
        diaSourceSchema.setFields(diaSourceFields);

        // diaObject remains mostly the same
        List<Schema.Field> diaObjectFields = List.of(
                new Schema.Field("diaObjectId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("nDiaSources", Schema.create(Schema.Type.INT))
        );
        Schema diaObjectSchema = Schema.createRecord("diaObject", null, "lsst.v11_2", false);
        diaObjectSchema.setFields(diaObjectFields);

        // diaForcedSource remains the same
        List<Schema.Field> diaForcedSourceFields = List.of(
                new Schema.Field("diaForcedSourceId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("diaObjectId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE)),
                new Schema.Field("visit", Schema.create(Schema.Type.LONG)),
                new Schema.Field("detector", Schema.create(Schema.Type.INT))
        );
        Schema diaForcedSourceSchema = Schema.createRecord("diaForcedSource", null, "lsst.v11_2", false);
        diaForcedSourceSchema.setFields(diaForcedSourceFields);

        // Top-level alert record (schema 1102)
        List<Schema.Field> alertFields = List.of(
                new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG)),
                new Schema.Field("observation_reason", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("target_name", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field("diaSource", diaSourceSchema),
                new Schema.Field("prvDiaForcedSources", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(diaForcedSourceSchema)))),
                new Schema.Field("diaObject", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), diaObjectSchema))),
                new Schema.Field("cutoutDifference", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)))),
                new Schema.Field("cutoutScience", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)))),
                new Schema.Field("cutoutTemplate", Schema.createUnion(Arrays.asList(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES))))
        );
        Schema alertSchema = Schema.createRecord("alert", null, "lsst.v11_2", false);
        alertSchema.setFields(alertFields);

        return alertSchema;
    }

    @Test
    void testSchema1100QueryGeneration() throws Exception {
        Schema schema1100 = createSchema1100();
        
        AlertsToKafka.Arguments args = createArguments("true", new java.util.LinkedHashSet<>(), null, null);

        String query = buildQuery(args, schema1100);

        System.out.println("Generated query for schema 1100:");
        System.out.println(query);
        System.out.println();

        // Verify the query structure
        assertTrue(query.contains("select"));
        assertTrue(query.contains("struct("));
        assertTrue(query.contains("as value"));
        assertTrue(query.contains("from lsst.alerts"));
        assertTrue(query.contains("where true"));

        // Verify it includes basic fields
        assertTrue(query.contains("diaSourceId"));
        assertTrue(query.contains("observation_reason"));

        // Verify it includes nested diaSource struct
        assertTrue(query.contains("diaSource"));
        assertTrue(query.contains("diaSource.diaSourceId as diaSourceId"));
        assertTrue(query.contains("diaSource.visit as visit"));
        assertTrue(query.contains("diaSource.ra as ra"));

        // CRITICAL: Schema 1100 should NOT include exposureTime or mixedInstruments
        assertFalse(query.contains("exposureTime"));
        assertFalse(query.contains("mixedInstruments"));
        assertFalse(query.contains("templateMjdMean"));
        assertFalse(query.contains("templateInstrument"));
    }

    @Test
    void testSchema1102QueryGeneration() throws Exception {
        Schema schema1102 = createSchema1102();
        
        AlertsToKafka.Arguments args = createArguments("true", new java.util.LinkedHashSet<>(), null, null);

        String query = buildQuery(args, schema1102);

        System.out.println("Generated query for schema 1102:");
        System.out.println(query);
        System.out.println();

        // Verify the query structure
        assertTrue(query.contains("select"));
        assertTrue(query.contains("struct("));
        assertTrue(query.contains("as value"));
        assertTrue(query.contains("from lsst.alerts"));

        // CRITICAL: Schema 1102 SHOULD include exposureTime and mixedInstruments
        assertTrue(query.contains("exposureTime"));
        assertTrue(query.contains("mixedInstruments"));
        assertTrue(query.contains("templateMjdMean"));
        assertTrue(query.contains("templateInstrument"));
        assertTrue(query.contains("templateVersion"));
    }

    @Test
    void testBackwardCompatibilityWithExclusions() throws Exception {
        Schema schema1100 = createSchema1100();
        
        java.util.Set<String> excludes = new java.util.LinkedHashSet<>(java.util.Set.of(
                "ssSource", "mpc_orbits", "cutoutTemplate", "cutoutScience", "cutoutDifference"));
        AlertsToKafka.Arguments args = createArguments("true", excludes, null, null);

        String query = buildQuery(args, schema1100);

        System.out.println("Generated query with default exclusions:");
        System.out.println(query);
        System.out.println();

        // Verify excluded fields are NOT in the query
        assertFalse(query.contains("ssSource"));
        assertFalse(query.contains("mpc_orbits"));
        assertFalse(query.contains("cutoutTemplate"));
        assertFalse(query.contains("cutoutScience"));
        assertFalse(query.contains("cutoutDifference"));

        // Verify included fields ARE in the query
        assertTrue(query.contains("diaSource"));
        assertTrue(query.contains("prvDiaForcedSources"));
        assertTrue(query.contains("diaObject"));
    }

    @Test
    void testCustomWhereClause() throws Exception {
        Schema schema1100 = createSchema1100();
        
        AlertsToKafka.Arguments args = createArguments("diaSource.diaObjectId > 0 AND diaSource.reliability > 0.95", 
                                                        new java.util.LinkedHashSet<>(), null, null);

        String query = buildQuery(args, schema1100);

        assertTrue(query.contains("where diaSource.diaObjectId > 0 AND diaSource.reliability > 0.95"));
    }

    @Test
    void testLimitAndOrderBy() throws Exception {
        Schema schema1100 = createSchema1100();
        
        AlertsToKafka.Arguments args = createArguments("true", new java.util.LinkedHashSet<>(), 100, "diaSourceId DESC");

        String query = buildQuery(args, schema1100);

        assertTrue(query.contains("order by diaSourceId DESC"));
        assertTrue(query.contains("limit 100"));
    }

    @Test
    void testSchemaEvolutionKeyScenario() throws Exception {
        // This is the KEY test: when querying from a newer Iceberg table
        // but serializing to an older Avro schema, the query should only
        // select fields present in the older schema
        
        Schema oldSchema = createSchema1100();  // Target: schema 1100 (no exposureTime)
        
        AlertsToKafka.Arguments args = createArguments("true", new java.util.LinkedHashSet<>(), null, null);

        String query = buildQuery(args, oldSchema);

        System.out.println("KEY SCENARIO: Query for schema 1100 (excluding new fields):");
        System.out.println(query);
        System.out.println();

        // Even though the Iceberg table might have exposureTime and mixedInstruments,
        // the query should NOT include them because they're not in schema 1100
        assertFalse(query.contains("exposureTime"), 
                "Query should NOT include exposureTime when targeting schema 1100");
        assertFalse(query.contains("mixedInstruments"), 
                "Query should NOT include mixedInstruments when targeting schema 1100");
        assertFalse(query.contains("templateMjdMean"), 
                "Query should NOT include templateMjdMean when targeting schema 1100");

        // But it should still include standard diaSource fields
        assertTrue(query.contains("diaSource.diaSourceId as diaSourceId"));
        assertTrue(query.contains("diaSource.visit as visit"));
        assertTrue(query.contains("diaSource.ra as ra"));
        assertTrue(query.contains("diaSource.psfFlux as psfFlux"));
    }

    @Test
    void testNestedStructFormatting() throws Exception {
        Schema schema1100 = createSchema1100();
        
        AlertsToKafka.Arguments args = createArguments("true", new java.util.LinkedHashSet<>(), null, null);

        String query = buildQuery(args, schema1100);

        // Verify proper formatting of nested structs
        assertTrue(query.contains("struct("));
        assertTrue(query.contains("diaSource.diaSourceId as diaSourceId"));
        assertTrue(query.contains("as diaSource"));
        assertTrue(query.contains("as diaObject"));
    }

    @Test
    void testEmptyExclusionList() throws Exception {
        Schema schema1100 = createSchema1100();
        
        AlertsToKafka.Arguments args = createArguments("true", new java.util.LinkedHashSet<>(), null, null);

        String query = buildQuery(args, schema1100);

        // With empty exclusion list, all fields should be included
        assertTrue(query.contains("cutoutDifference"));
        assertTrue(query.contains("cutoutScience"));
        assertTrue(query.contains("cutoutTemplate"));
    }

    @Test
    void testAllFieldsExcludedThrowsError() throws Exception {
        Schema schema1100 = createSchema1100();
        
        java.util.Set<String> allFields = new java.util.LinkedHashSet<>(java.util.Set.of(
                "diaSourceId", "observation_reason", "target_name", "diaSource", 
                "prvDiaForcedSources", "diaObject", "cutoutDifference", 
                "cutoutScience", "cutoutTemplate"));
        AlertsToKafka.Arguments args = createArguments("true", allFields, null, null);

        // Expect InvocationTargetException wrapping IllegalArgumentException
        Exception exception = assertThrows(Exception.class, () -> {
            buildQuery(args, schema1100);
        });
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertTrue(exception.getCause().getMessage().contains("Schema exclusions removed every schema field"));
    }
}
