package paimon.debug;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
/**
 * @projectName: FiveMinutesAbcData
 * @className: CreateCatalog
 * @description: paimon.debug.CreateCatalog
 * @author: jiaqing.he
 * @date: 2023/10/1 19:40
 * @version: 1.0
 */
public class CreateCatalog {

    public static void main(String[] args) {
        Catalog filesystemCatalog = createFilesystemCatalog();
        System.out.println(filesystemCatalog);
    }

    public static Catalog createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("file:/tmp/paimon_test"));
        return CatalogFactory.createCatalog(context);
    }

    public static Catalog createHiveCatalog() {
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



}
