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
        Iterator<Block> r_it_test = relation.iterator();

        int r_size = relation.getEstimatedSize();
        System.out.println(r_size);


        ArrayList<Block>[] sublists = new ArrayList[(int) Math.ceil(r_size/bm.getFreeBlocks())];
        int current_list = 0;
        ArrayList<Block> blocks = new ArrayList<>();
        ColumnDefinition cd = relation.getColumns();;
        int sort_index = getSortColumnIndex();

        for (Iterator<Block> r_it = relation.iterator(); r_it.hasNext();) {
            Block b = r_it.next();
            bm.load(b);
            blocks.add(b);
            //System.out.println(bm.getUsedBlocks());
            if (bm.getFreeBlocks() == 0){
                BlockSorter.INSTANCE.sort(relation, blocks, cd.getColumnComparator(sort_index));
                //System.out.println(blocks);
                sublists[current_list] = new ArrayList<>();
                for (Block block:
                        blocks) {
                    //output.output(b);
                    sublists[current_list].add(block);
                    bm.release(block, false);
                }
                current_list++;
                blocks.clear();
            }
        }
        if (!blocks.isEmpty()) {
            BlockSorter.INSTANCE.sort(relation, blocks, cd.getColumnComparator(sort_index));
            for (Block b :
                    blocks) {
                bm.release(b, true);
            }
        }
        System.out.println(sublists[0].iterator());

//        Block b = r_it_test.next();
//        bm.load(b);
//        System.out.println(b);
//        bm.release(b, true);
//        Iterator<Block> r_it_test2 = relation.iterator();
//        Block c = r_it_test2.next();
//        bm.load(c);
//        System.out.println(c);


        //throw new UnsupportedOperationException("TODO");
    }
}