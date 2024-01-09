package paimon.debug;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcSinkBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.Test;

import static org.apache.paimon.types.RowKind.INSERT;

/**
 * @projectName: FiveMinutesAbcData
 * @className: WriteCdcToTable
 * @description: flink.debug.paimon.WriteCdcToTable
 * @author: jiaqing.he
 * @date: 2023/9/16 21:34
 * @version: 1.0
 */
public class TestWriteCdcToTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RichCdcRecord> dataStream =
                env.fromElements(
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "123")
                                .field("price", DataTypes.DOUBLE(), "62.2")
                                .build(),
                        // dt field will be added with schema evolution
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "245")
                                .field("price", DataTypes.DOUBLE(), "82.1")
                                .field("dt", DataTypes.TIMESTAMP(), "2023-06-12 20:21:12")
                                .build());

        Identifier identifier = Identifier.create("my_db", "T");
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", "file:/tmp/paimon/warehouse");
        Catalog.Loader catalogLoader =
                () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        new RichCdcSinkBuilder()
                .withInput(dataStream)
                .withTable(createTableIfNotExists(identifier))
                .withIdentifier(identifier)
                .withCatalogLoader(catalogLoader)
                .build();

        env.execute();
    }

    private static Table createTableIfNotExists(Identifier identifier) throws Exception {
        CatalogContext context = CatalogContext.create(new Path("..."));
        Catalog catalog = CatalogFactory.createCatalog(context);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("order_id");
        schemaBuilder.column("order_id", DataTypes.BIGINT());
        schemaBuilder.column("price", DataTypes.DOUBLE());
        Schema schema = schemaBuilder.build();
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        }
        return catalog.getTable(identifier);
    }


    @Test
    public void testDataStreamCdc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<RichCdcRecord> dataStream =
                env.fromElements(
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "123")
                                .field("price", DataTypes.DOUBLE(), "62.2")
                                .build(),
                        // dt field will be added with schema evolution
                        RichCdcRecord.builder(INSERT)
                                .field("order_id", DataTypes.BIGINT(), "245")
                                .field("price", DataTypes.DOUBLE(), "82.1")
                                .field("dt", DataTypes.TIMESTAMP(), "2023-06-12 20:21:12")
                                .build());

        CatalogContext context = CatalogContext.create(new Path("file:/tmp/paimon_test"));
        Catalog catalog = CatalogFactory.createCatalog(context);
        System.out.println(catalog);
        catalog.dropDatabase("my_db", true, true);
        System.out.println("my_db是否存在: " + catalog.databaseExists("my_db"));
        catalog.createDatabase("my_db", false);


        Identifier identifier = Identifier.create("my_db", "test_order");
        Options catalogOptions = context.options();
//        catalogOptions.set("warehouse", "file:/tmp/paimon/warehouse");
//        catalogOptions = context.options();
        Catalog.Loader catalogLoader = () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("order_id");
        schemaBuilder.column("order_id", DataTypes.BIGINT());
        schemaBuilder.column("price", DataTypes.DOUBLE());
        Schema schema = schemaBuilder.build();
        try {
            catalog.createTable(identifier, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
            e.printStackTrace();
            throw e;
        }
        Table paimonTable = catalog.getTable(identifier);

        new RichCdcSinkBuilder()
                .withInput(dataStream)
                .withTable(paimonTable)
                .withIdentifier(identifier)
                .withCatalogLoader(catalogLoader)
                .build();

        env.execute();

    }



}
