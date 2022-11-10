package exercise1;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.dbms.utils.BlockSorter;
import de.hpi.dbs2.exercise1.SortOperation;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@ChosenImplementation(true)
public class TPMMSJava extends SortOperation {
    public TPMMSJava(@NotNull BlockManager manager, int sortColumnIndex) {
        super(manager, sortColumnIndex);
    }

    @Override
    public int estimatedIOCost(@NotNull Relation relation) {

        System.out.println("erqewljrlkqwjr");
        return 1;
        //throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void sort(@NotNull Relation relation, @NotNull BlockOutput output) {
        BlockManager bm = getBlockManager();
        Iterator<Block> r_it = relation.iterator();


        System.out.println(bm);
        int freeBlocks = bm.getFreeBlocks();

/*        Block test1 = r_it.next();
        bm.load(test1);
        System.out.println(bm);
        System.out.println(test1.iterator().next());*/

        //for (int i = 0; i < freeBlocks; i++) {
            Block b = r_it.next();
            bm.load(b);
            int block_size = b.getSize();
            List<Tuple> tuples = new ArrayList<>();
            List<Block> block = new ArrayList<>();
            block.add(b);
            Comparator<Tuple> byIndex = (p1, p2) -> p1.get(0) - p2.get(0);;

            // BlockSorter.sort(relation, block, byIndex);

            BlockSorter.INSTANCE.sort(relation, block, byIndex);
            for (int j = 0; j < block_size; j++) {
                Tuple t = b.iterator().next();
                System.out.println(t.get(0).getClass());
                tuples.add(t);
           // }

        }
        //throw new UnsupportedOperationException("TODO");
    }
}