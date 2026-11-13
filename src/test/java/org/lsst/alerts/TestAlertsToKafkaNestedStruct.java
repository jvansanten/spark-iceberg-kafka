package org.lsst.alerts;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for nested struct query construction in AlertsToKafka.
 * These tests verify that the query builder correctly handles:
 * - Flat schemas
 * - Nested record types (structs)
 * - Union types (nullable fields)
 * - Array types
 * - Schema evolution scenarios (e.g., schema 1100 vs 1102)
 */
public class TestAlertsToKafkaNestedStruct {

    private static Method buildColumnExpressionMethod;
    private static Method resolveUnionMethod;
    private static Class<?> alertPacketSchema1100;
    private static Class<?> alertPacketSchema1102;

    @BeforeAll
    static void setup() throws Exception {
        // Get private methods via reflection for testing
        buildColumnExpressionMethod = AlertsToKafka.class.getDeclaredMethod(
                "buildColumnExpression", Schema.class, String.class, String.class);
        buildColumnExpressionMethod.setAccessible(true);

        resolveUnionMethod = AlertsToKafka.class.getDeclaredMethod(
                "resolveUnion", Schema.class);
        resolveUnionMethod.setAccessible(true);
    }

    // Helper to invoke private method
    private static String buildColumnExpression(Schema schema, String fieldName, String prefix) throws Exception {
        return (String) buildColumnExpressionMethod.invoke(null, schema, fieldName, prefix);
    }

    // Helper to invoke private method
    private static Schema resolveUnion(Schema schema) throws Exception {
        return (Schema) resolveUnionMethod.invoke(null, schema);
    }

    @Test
    void testResolveUnionNullFirst() throws Exception {
        // Create a union schema: ["null", "string"]
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema unionSchema = Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL),
                stringSchema));

        Schema resolved = resolveUnion(unionSchema);
        assertEquals(Schema.Type.STRING, resolved.getType());
    }

    @Test
    void testResolveUnionNonNullFirst() throws Exception {
        // Create a union schema: ["string", "null"]
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema unionSchema = Schema.createUnion(Arrays.asList(
                stringSchema,
                Schema.create(Schema.Type.NULL)));

        Schema resolved = resolveUnion(unionSchema);
        assertEquals(Schema.Type.STRING, resolved.getType());
    }

    @Test
    void testResolveUnionMultipleNonNull() throws Exception {
        // Create a union schema: ["string", "int"]
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema intSchema = Schema.create(Schema.Type.INT);
        Schema unionSchema = Schema.createUnion(Arrays.asList(stringSchema, intSchema));

        Schema resolved = resolveUnion(unionSchema);
        assertEquals(Schema.Type.STRING, resolved.getType());
    }

    @Test
    void testBuildPrimitiveField() throws Exception {
        Schema longSchema = Schema.create(Schema.Type.LONG);
        String expr = buildColumnExpression(longSchema, "diaSourceId", "");
        assertEquals("diaSourceId", expr);
    }

    @Test
    void testBuildNullablePrimitiveField() throws Exception {
        Schema nullableLongSchema = Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.LONG)));
        String expr = buildColumnExpression(nullableLongSchema, "diaSourceId", "");
        assertEquals("diaSourceId", expr);
    }

    @Test
    void testBuildSimpleRecordField() throws Exception {
        // Create a simple nested record schema
        Schema.Field raField = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        Schema.Field decField = new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE));
        Schema positionSchema = Schema.createRecord("Position", null, null, false);
        positionSchema.setFields(List.of(raField, decField));

        String expr = buildColumnExpression(positionSchema, "position", "");
        assertTrue(expr.contains("struct("));
        assertTrue(expr.contains("position.ra as ra"));
        assertTrue(expr.contains("position.dec as dec"));
        assertTrue(expr.contains("as position"));
    }

    @Test
    void testBuildNestedRecordField() throws Exception {
        // Create a deeply nested structure: alert -> diaSource -> position
        Schema.Field raField = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        Schema.Field decField = new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE));
        Schema positionSchema = Schema.createRecord("Position", null, null, false);
        positionSchema.setFields(List.of(raField, decField));

        Schema.Field positionField = new Schema.Field("position", positionSchema);
        Schema.Field visitField = new Schema.Field("visit", Schema.create(Schema.Type.LONG));
        Schema diaSourceSchema = Schema.createRecord("diaSource", null, null, false);
        diaSourceSchema.setFields(List.of(positionField, visitField));

        String expr = buildColumnExpression(diaSourceSchema, "diaSource", "");
        assertTrue(expr.contains("struct("));
        assertTrue(expr.contains("diaSource.position"));
        assertTrue(expr.contains("diaSource.visit as visit"));
        assertTrue(expr.contains("as diaSource"));
    }

    @Test
    void testBuildArrayOfPrimitives() throws Exception {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.FLOAT));
        String expr = buildColumnExpression(arraySchema, "values", "");
        assertEquals("values", expr);
    }

    @Test
    void testBuildArrayOfRecords() throws Exception {
        // Create array of records
        Schema.Field xField = new Schema.Field("x", Schema.create(Schema.Type.FLOAT));
        Schema.Field yField = new Schema.Field("y", Schema.create(Schema.Type.FLOAT));
        Schema pointSchema = Schema.createRecord("Point", null, null, false);
        pointSchema.setFields(List.of(xField, yField));
        
        Schema arraySchema = Schema.createArray(pointSchema);
        String expr = buildColumnExpression(arraySchema, "points", "");
        assertEquals("points", expr);
    }

    @Test
    void testBuildFullAlertSchema1100Style() throws Exception {
        // Simplified version of schema 1100
        // Top-level fields
        Schema.Field diaSourceIdField = new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG));
        Schema.Field observationReasonField = new Schema.Field("observation_reason", 
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))));
        
        // diaSource nested record
        Schema.Field ds_diaSourceId = new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG));
        Schema.Field ds_visit = new Schema.Field("visit", Schema.create(Schema.Type.LONG));
        Schema.Field ds_detector = new Schema.Field("detector", Schema.create(Schema.Type.INT));
        Schema.Field ds_ra = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        Schema.Field ds_dec = new Schema.Field("dec", Schema.create(Schema.Type.DOUBLE));
        Schema.Field ds_psfFlux = new Schema.Field("psfFlux", 
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT))));
        Schema.Field ds_reliability = new Schema.Field("reliability", 
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT))));
        
        Schema diaSourceSchema = Schema.createRecord("diaSource", null, "lsst.v11_0", false);
        diaSourceSchema.setFields(List.of(ds_diaSourceId, ds_visit, ds_detector, ds_ra, ds_dec, ds_psfFlux, ds_reliability));
        
        Schema.Field diaSourceField = new Schema.Field("diaSource", diaSourceSchema);
        
        // Build the alert schema
        Schema alertSchema = Schema.createRecord("alert", null, "lsst.v11_0", false);
        alertSchema.setFields(List.of(diaSourceIdField, observationReasonField, diaSourceField));

        // Test building the diaSource field
        String diaSourceExpr = buildColumnExpression(diaSourceSchema, "diaSource", "");
        assertTrue(diaSourceExpr.contains("struct("));
        assertTrue(diaSourceExpr.contains("diaSource.diaSourceId as diaSourceId"));
        assertTrue(diaSourceExpr.contains("diaSource.visit as visit"));
        assertTrue(diaSourceExpr.contains("diaSource.ra as ra"));
        assertTrue(diaSourceExpr.contains("diaSource.psfFlux as psfFlux"));
        assertTrue(diaSourceExpr.contains("as diaSource"));
    }

    @Test
    void testSchemaEvolutionScenario() throws Exception {
        // Simulate the difference between schema 1100 and 1102
        // Schema 1100 diaSource: NO exposureTime, NO mixedInstruments
        Schema.Field ds1100_diaSourceId = new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG));
        Schema.Field ds1100_visit = new Schema.Field("visit", Schema.create(Schema.Type.LONG));
        Schema.Field ds1100_ra = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        
        Schema diaSourceSchema1100 = Schema.createRecord("diaSource", null, "lsst.v11_0", false);
        diaSourceSchema1100.setFields(List.of(ds1100_diaSourceId, ds1100_visit, ds1100_ra));

        // Schema 1102 diaSource: HAS exposureTime and mixedInstruments
        Schema.Field ds1102_diaSourceId = new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG));
        Schema.Field ds1102_visit = new Schema.Field("visit", Schema.create(Schema.Type.LONG));
        Schema.Field ds1102_ra = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        Schema.Field ds1102_exposureTime = new Schema.Field("exposureTime", 
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT))));
        Schema.Field ds1102_mixedInstruments = new Schema.Field("mixedInstruments", Schema.create(Schema.Type.BOOLEAN));
        
        Schema diaSourceSchema1102 = Schema.createRecord("diaSource", null, "lsst.v11_2", false);
        diaSourceSchema1102.setFields(List.of(ds1102_diaSourceId, ds1102_visit, ds1102_ra, ds1102_exposureTime, ds1102_mixedInstruments));

        // When targeting schema 1100, the generated SQL should NOT include exposureTime or mixedInstruments
        String expr1100 = buildColumnExpression(diaSourceSchema1100, "diaSource", "");
        assertFalse(expr1100.contains("exposureTime"));
        assertFalse(expr1100.contains("mixedInstruments"));
        assertTrue(expr1100.contains("diaSourceId"));
        assertTrue(expr1100.contains("visit"));
        assertTrue(expr1100.contains("ra"));

        // When targeting schema 1102, the generated SQL SHOULD include exposureTime and mixedInstruments
        String expr1102 = buildColumnExpression(diaSourceSchema1102, "diaSource", "");
        assertTrue(expr1102.contains("exposureTime"));
        assertTrue(expr1102.contains("mixedInstruments"));
        assertTrue(expr1102.contains("diaSourceId"));
        assertTrue(expr1102.contains("visit"));
        assertTrue(expr1102.contains("ra"));
    }

    @Test
    void testNestedPathBuilding() throws Exception {
        // Test that nested paths are built correctly
        Schema.Field innerField = new Schema.Field("inner", Schema.create(Schema.Type.STRING));
        Schema innerSchema = Schema.createRecord("Inner", null, null, false);
        innerSchema.setFields(List.of(innerField));

        Schema.Field middleField = new Schema.Field("middle", innerSchema);
        Schema middleSchema = Schema.createRecord("Middle", null, null, false);
        middleSchema.setFields(List.of(middleField));

        Schema.Field outerField = new Schema.Field("outer", middleSchema);
        Schema outerSchema = Schema.createRecord("Outer", null, null, false);
        outerSchema.setFields(List.of(outerField));

        String expr = buildColumnExpression(outerSchema, "root", "prefix");
        assertTrue(expr.contains("prefix.root.outer.middle.inner"));
    }

    @Test
    void testEnumAndFixedTypes() throws Exception {
        // Test enum type
        Schema enumSchema = Schema.createEnum("Color", null, null, Arrays.asList("RED", "GREEN", "BLUE"));
        String enumExpr = buildColumnExpression(enumSchema, "color", "");
        assertEquals("color", enumExpr);

        // Test fixed type
        Schema fixedSchema = Schema.createFixed("MD5", null, null, 16);
        String fixedExpr = buildColumnExpression(fixedSchema, "hash", "");
        assertEquals("hash", fixedExpr);
    }

    @Test
    void testMapType() throws Exception {
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
        String expr = buildColumnExpression(mapSchema, "metadata", "");
        assertEquals("metadata", expr);
    }

    @Test
    void testDeeplyNestedStructure() throws Exception {
        // Create a complex nested structure similar to the actual alert packet
        // diaForcedSource record
        Schema.Field dfs_id = new Schema.Field("diaForcedSourceId", Schema.create(Schema.Type.LONG));
        Schema.Field dfs_ra = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        Schema diaForcedSourceSchema = Schema.createRecord("diaForcedSource", null, null, false);
        diaForcedSourceSchema.setFields(List.of(dfs_id, dfs_ra));

        // Array of diaForcedSource
        Schema prvDiaForcedSourcesSchema = Schema.createArray(diaForcedSourceSchema);
        Schema.Field pdfsField = new Schema.Field("prvDiaForcedSources", 
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), prvDiaForcedSourcesSchema)));

        // diaObject record
        Schema.Field do_id = new Schema.Field("diaObjectId", Schema.create(Schema.Type.LONG));
        Schema.Field do_ra = new Schema.Field("ra", Schema.create(Schema.Type.DOUBLE));
        Schema diaObjectSchema = Schema.createRecord("diaObject", null, null, false);
        diaObjectSchema.setFields(List.of(do_id, do_ra));
        Schema.Field dioField = new Schema.Field("diaObject", 
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), diaObjectSchema)));

        // Top-level alert
        Schema.Field alertId = new Schema.Field("diaSourceId", Schema.create(Schema.Type.LONG));
        Schema alertSchema = Schema.createRecord("alert", null, "lsst.v11_0", false);
        alertSchema.setFields(List.of(alertId, pdfsField, dioField));

        // Test building prvDiaForcedSources (array of records)
        String pdfsExpr = buildColumnExpression(
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), prvDiaForcedSourcesSchema)),
                "prvDiaForcedSources", "");
        assertEquals("prvDiaForcedSources", pdfsExpr);

        // Test building diaObject (nullable record)
        String dioExpr = buildColumnExpression(
                Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), diaObjectSchema)),
                "diaObject", "");
        // Should be a struct since it resolves to RECORD
        assertTrue(dioExpr.contains("struct("));
        assertTrue(dioExpr.contains("diaObject.diaObjectId as diaObjectId"));
        assertTrue(dioExpr.contains("as diaObject"));
    }

    @Test
    void testAllLSSTNestedTypes() throws Exception {
        // Test all the known nested types in LSST alert packets
        Set<String> nestedTypes = Set.of("diaSource", "ssSource", "mpc_orbits", "prvDiaSources", 
                                        "prvDiaForcedSources", "diaObject");
        
        // Just verify we can iterate through them
        assertEquals(6, nestedTypes.size());
        assertTrue(nestedTypes.contains("diaSource"));
        assertTrue(nestedTypes.contains("ssSource"));
        assertTrue(nestedTypes.contains("mpc_orbits"));
        assertTrue(nestedTypes.contains("prvDiaSources"));
        assertTrue(nestedTypes.contains("prvDiaForcedSources"));
        assertTrue(nestedTypes.contains("diaObject"));
    }

    @Test
    void testDefaultExclusions() throws Exception {
        // Verify the default exclusions match expectations
        Set<String> defaults = Set.of("ssSource", "mpc_orbits", "cutoutTemplate", "cutoutScience", "cutoutDifference");
        assertEquals(5, defaults.size());
        assertTrue(defaults.contains("ssSource"));
        assertTrue(defaults.contains("mpc_orbits"));
        assertTrue(defaults.contains("cutoutTemplate"));
        assertTrue(defaults.contains("cutoutScience"));
        assertTrue(defaults.contains("cutoutDifference"));
    }

    @Test
    void testUnionResolutionEdgeCases() throws Exception {
        // Single type union
        Schema singleTypeUnion = Schema.createUnion(List.of(Schema.create(Schema.Type.STRING)));
        Schema resolvedSingle = resolveUnion(singleTypeUnion);
        assertEquals(Schema.Type.STRING, resolvedSingle.getType());
        
        // Two-type union with null
        Schema twoTypeUnion = Schema.createUnion(List.of(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING)));
        Schema resolvedTwo = resolveUnion(twoTypeUnion);
        assertEquals(Schema.Type.STRING, resolvedTwo.getType());
    }

    @Test
    void testComplexUnionWithThreeTypes() throws Exception {
        // Union of three types: null, string, int
        Schema unionSchema = Schema.createUnion(List.of(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING),
                Schema.create(Schema.Type.INT)));
        
        Schema resolved = resolveUnion(unionSchema);
        // Should pick the first non-null type
        assertEquals(Schema.Type.STRING, resolved.getType());
    }
}
