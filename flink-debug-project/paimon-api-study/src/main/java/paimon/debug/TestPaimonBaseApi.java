package paimon.debug;

import com.google.common.collect.Lists;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestPaimonBaseApi
 * @description: paimon.debug.TestPaimonBaseApi
 * @author: jiaqing.he
 * @date: 2023/10/1 19:46
 * @version: 1.0
 */
public class TestPaimonBaseApi {

    @Test
    public void testCreateCatalog() {
        CatalogContext context = CatalogContext.create(new Path("file:/tmp/paimon_test"));
        Catalog filesystemCatalog = CatalogFactory.createCatalog(context);
        System.out.println(filesystemCatalog);

    }

    public Catalog createHiveCatalog() {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        options.set("warehouse", "...");
        options.set("metastore", "hive");
        options.set("uri", "...");
        options.set("hive-conf-dir", "...");
        options.set("hadoop-conf-dir", "...");
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    @Test
    public void testCatalogDatabaseTable() throws Exception {
        CatalogContext context = CatalogContext.create(new Path("file:/tmp/paimon_test"));
        Catalog catalog = CatalogFactory.createCatalog(context);
        System.out.println(catalog);
        catalog.dropDatabase("my_db", true, true);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        catalog.createDatabase("my_db", false);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        System.out.println("listDatabases 已有DBs: " + catalog.listDatabases());

        // 2.
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("f0", "f1");
        schemaBuilder.partitionKeys("f1");
        schemaBuilder.column("f0", DataTypes.STRING());
        schemaBuilder.column("f1", DataTypes.INT());
//        Schema schema = schemaBuilder.build();
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("compaction.max.file-num", "40");

        Schema schema = new Schema(
                Lists.newArrayList(
                        new DataField(0, "col1", DataTypes.STRING(), "field1"),
                        new DataField(1, "col2", DataTypes.STRING(), "field2"),
                        new DataField(2, "col3", DataTypes.STRING(), "field3"),
                        new DataField(3, "col4", DataTypes.BIGINT(), "field4"),
                        new DataField(
                                4,
                                "col5",
                                DataTypes.ROW(
                                        new DataField(
                                                5, "f1", DataTypes.STRING(), "f1"),
                                        new DataField(
                                                6, "f2", DataTypes.STRING(), "f2"),
                                        new DataField(
                                                7, "f3", DataTypes.STRING(), "f3")),
                                "field5"),
                        new DataField(8, "col6", DataTypes.STRING(), "field6")),
                Lists.newArrayList("col1"), // partition keys
                Lists.newArrayList("col1", "col2"), // primary key
                options,
                "table comment");

        Identifier identifier = Identifier.create("my_db", "my_table");
        catalog.dropTable(identifier, true);
        System.out.println("my_db.my_table 是否存在: " + catalog.tableExists(identifier));
        catalog.createTable(identifier, schema, false);
        System.out.println("my_db.my_table 是否存在: " + catalog.tableExists(identifier));
        Table table = catalog.getTable(identifier);

        Identifier newTableName = Identifier.create("my_db", "my_renamed_table");
        catalog.renameTable(identifier, newTableName, true);
        Table newTable = catalog.getTable(newTableName);
        System.out.println("my_db.my_renamed_table 是否存在: " + catalog.tableExists(newTableName));

        // 修改表结构


        // add option
        SchemaChange addOption = SchemaChange.setOption("snapshot.time-retained", "2h");
        // remove option
        SchemaChange removeOption = SchemaChange.removeOption("compaction.max.file-num");
        // add column
        SchemaChange addColumn = SchemaChange.addColumn("col1_after", DataTypes.STRING());
        // add a column after col1
        SchemaChange.Move after = SchemaChange.Move.after("col1_after", "col1");
        SchemaChange addColumnAfterField =
                SchemaChange.addColumn("col7", DataTypes.STRING(), "", after);
        // rename column
        SchemaChange renameColumn = SchemaChange.renameColumn("col3", "col3_new_name");
        // drop column
        SchemaChange dropColumn = SchemaChange.dropColumn("col6");
        // update column comment
        SchemaChange updateColumnComment =
                SchemaChange.updateColumnComment(new String[] {"col4"}, "col4 field");
        // update nested column comment
        SchemaChange updateNestedColumnComment =
                SchemaChange.updateColumnComment(new String[] {"col5", "f1"}, "col5 f1 field");
        // update column type
        SchemaChange updateColumnType = SchemaChange.updateColumnType("col4", DataTypes.DOUBLE());
        // update column position, you need to pass in a parameter of type Move
        SchemaChange updateColumnPosition =
                SchemaChange.updateColumnPosition(SchemaChange.Move.first("col4"));
        // update column nullability
        SchemaChange updateColumnNullability =
                SchemaChange.updateColumnNullability(new String[] {"col4"}, false);
        // update nested column nullability
        SchemaChange updateNestedColumnNullability =
                SchemaChange.updateColumnNullability(new String[] {"col5", "f2"}, false);

        SchemaChange[] schemaChanges = new SchemaChange[] {
                        addOption,
                        removeOption,
                        addColumn,
                        addColumnAfterField,
                        renameColumn,
                        dropColumn,
                        updateColumnComment,
                        updateNestedColumnComment,
                        updateColumnType,
                        updateColumnPosition,
                        updateColumnNullability,
                        updateNestedColumnNullability
                };



        table = catalog.getTable(newTableName);
        System.out.println(String.format("%s : Alert修改Schema前 \n\t ops=%s, pkeys=%s, \n\t rowType=%s ",
                newTableName, table.options(), table.primaryKeys(), table.rowType()));

        catalog.alterTable(newTableName, Arrays.asList(schemaChanges), false);

        table = catalog.getTable(newTableName);
        System.out.println(String.format("%s : Alert修改Schema后 \n\t ops=%s, pkeys=%s, \n\t rowType=%s ",
                newTableName, table.options(), table.primaryKeys(), table.rowType()));


    }

    @Test
    public void testBatchReadWriteData() throws Exception {
        CatalogContext context = CatalogContext.create(new Path("file:/tmp/paimon_test"));
        Catalog catalog = CatalogFactory.createCatalog(context);
        System.out.println(catalog);
        catalog.dropDatabase("my_db", true, true);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        catalog.createDatabase("my_db", false);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        System.out.println("listDatabases 已有DBs: " + catalog.listDatabases());


        Identifier identifier = Identifier.create("my_db", "my_table");
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .primaryKey("name")
                .options(new HashMap(){{
//                    put("bucket", "4");
                    put("compaction.max.file-num", "40");
                }})
                .build();
        catalog.dropTable(identifier, true);
        catalog.createTable(identifier, schema, false);

        // 1. Get Table and Create BatchWriter Builder
        Table table = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
        // 2. Write records in distributed tasks
        BatchTableWrite write = writeBuilder.newWrite();

        for (int i = 0; i < 10; i++) {
            GenericRow record = GenericRow.of(BinaryString.fromString("Name_" + i), i);
            write.write(record);
        }
        List<CommitMessage> messages = write.prepareCommit();

        // 3. Collect all CommitMessages to a global node and commit
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);

        // Abort unsuccessful commit to delete data files
        // commit.abort(messages);

        // Batch Reader
        PredicateBuilder builder = new PredicateBuilder(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        Predicate notNull = builder.isNotNull(0);
        Predicate greaterOrEqual = builder.greaterOrEqual(1, 12);

        int[] projection = new int[] {0, 1};

        ReadBuilder readBuilder = table.newReadBuilder()
                        .withProjection(projection)
                        .withFilter(Lists.newArrayList(notNull, greaterOrEqual));

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        List<Split> splits = readBuilder.newScan().plan().splits();

        // 3. Distribute these splits to different tasks

        // 4. Read a split in task
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);

        RecordReader.RecordIterator<InternalRow> it = reader.readBatch();
        InternalRow next;
        while ((next=it.next()) != null) {
            BinaryString name = next.getString(0);
            int age = next.getInt(1);
            System.out.println(next + ", name=" + name + ", age="+age);
        }

    }


    @Test
    public void testStreamReadWriteData() throws Exception {
        CatalogContext context = CatalogContext.create(new Path("file:/tmp/paimon_test"));
        Catalog catalog = CatalogFactory.createCatalog(context);
        System.out.println(catalog);
        catalog.dropDatabase("my_db", true, true);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        catalog.createDatabase("my_db", false);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        System.out.println("listDatabases 已有DBs: " + catalog.listDatabases());


        Identifier identifier = Identifier.create("my_db", "my_table");
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .primaryKey("name")
                .options(new HashMap(){{
//                    put("bucket", "4");
                    put("compaction.max.file-num", "40");
                }})
                .build();
        catalog.dropTable(identifier, true);
        catalog.createTable(identifier, schema, false);

        // 1. Get Table and Create BatchWriter Builder
        Table table = catalog.getTable(identifier);

        Executors.newSingleThreadScheduledExecutor().submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
                            // 2. Write records in distributed tasks
                            StreamTableWrite write = writeBuilder.newWrite();
                            // commitIdentifier like Flink checkpointId
                            int commitIdentifier = 0;
                            while (true) {
                                for (int i = 0; i < 10; i++) {
                                    GenericRow record = GenericRow.of(BinaryString.fromString("Name_" + i), 100 * commitIdentifier + i);
                                    write.write(record);
                                }
                                List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
                                commitIdentifier++;
                                // 3. Collect all CommitMessages to a global node and commit
                                StreamTableCommit commit = writeBuilder.newCommit();
                                commit.commit(commitIdentifier, messages);
                                Thread.sleep(1000);
                            }
                        } catch (Exception e) {

                        }
                    }
        });


        PredicateBuilder builder = new PredicateBuilder(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        Predicate notNull = builder.isNotNull(0);
        Predicate greaterOrEqual = builder.greaterOrEqual(1, 12);
        int[] projection = new int[] {0, 1};
        ReadBuilder readBuilder = table.newReadBuilder()
                .withProjection(projection)
                .withFilter(Lists.newArrayList(notNull, greaterOrEqual));

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
//        List<Split> splits = readBuilder.newScan().plan().splits();
        StreamTableScan scan = readBuilder.newStreamScan();

        while (true) {
            List<Split> splits = scan.plan().splits();
            // Distribute these splits to different tasks
            Long state = scan.checkpoint();
            // can be restored in scan.restore(state) after fail over

            // 3. Read a split in task
            TableRead read = readBuilder.newRead();
            RecordReader<InternalRow> reader = read.createReader(splits);
            reader.forEachRemaining(row -> {
                System.out.println(row + ", name=" + row.getString(0) + ", age="+row.getInt(1));
            });

            Thread.sleep(1000);
        }


    }



}
