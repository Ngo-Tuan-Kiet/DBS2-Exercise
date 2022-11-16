package exercise1;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.dbms.utils.BlockSorter;
import de.hpi.dbs2.exercise1.SortOperation;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.IntStream;

import static com.google.common.collect.Iterators.contains;

@ChosenImplementation(true)
public class TPMMSJava extends SortOperation {
    public TPMMSJava(@NotNull BlockManager manager, int sortColumnIndex) {
        super(manager, sortColumnIndex);
    }

    @Override
    public int estimatedIOCost(@NotNull Relation relation) {
        BlockManager bm = getBlockManager();

        if (relation.getEstimatedSize() > bm.getFreeBlocks() * bm.getFreeBlocks())
            throw new RelationSizeExceedsCapacityException();
        return 4 * relation.getEstimatedSize();
        //throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void sort(@NotNull Relation relation, @NotNull BlockOutput output) {
        BlockManager bm = getBlockManager();

        if (relation.getEstimatedSize() > bm.getFreeBlocks() * bm.getFreeBlocks())
            throw new RelationSizeExceedsCapacityException();

        int r_size = relation.getEstimatedSize();
        ArrayList<ArrayList<Block> > sublists = new ArrayList<>((int) Math.ceil(r_size/bm.getFreeBlocks()));
        int current_list = 0;
        ArrayList<Block> blocks = new ArrayList<>();
        ColumnDefinition cd = relation.getColumns();
        int sort_index = getSortColumnIndex();
        int block_cap = 0;

        /*
        Phase 1 of TPMMS,
        loading blocks into memory until full, sort, write it back and repeat
         */
        for (Iterator<Block> r_it = relation.iterator(); r_it.hasNext();) {
            Block b = r_it.next();
            bm.load(b);
            block_cap = b.getCapacity();
            blocks.add(b);
            if (bm.getFreeBlocks() == 0){
                BlockSorter.INSTANCE.sort(relation, blocks, cd.getColumnComparator(sort_index));
                System.out.println(blocks);
                sublists.add(current_list, new ArrayList<>());
                for (Block block:
                        blocks) {
                    sublists.get(current_list).add(block);
                    bm.release(block, true);
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


        /*
        Phase 2 of TPMMS
         */
        Iterator<Block>[] iters = new Iterator[sublists.size()];
        for (int i = 0; i < sublists.size(); i++) {
            iters[i] = sublists.get(i).iterator();
        }

        Block out_block = bm.allocate(true);
        Block[] first_blocks = new Block[sublists.size()];
        Iterator<Tuple>[] block_iters = new Iterator[sublists.size()];
        ArrayList<Tuple> al = new ArrayList<>(sublists.size());
        Tuple first_tuple;

        /*
        load first subblock of each sublist into memory
         */
        if (Arrays.stream(first_blocks).anyMatch(x -> Objects.isNull(x))){
            int[] empty_blocks = IntStream.range(0, sublists.size())
                    .filter(x -> first_blocks[x] == null).toArray();
            //System.out.println(empty_blocks.length);
            for (int j = 0; j < empty_blocks.length; j++) {
                int k = empty_blocks[j];
                first_blocks[k] = iters[k].next();
                bm.load(first_blocks[k]);
                block_iters[k] = first_blocks[k].iterator();
                al.add(k, block_iters[k].next());
            }
        }
        System.out.println();

        /*
        merging phase
         */
        for (int i = 0; i < r_size*block_cap; i++) {

            /*
            if there are not tuples of a loaded block for a sublist, then load next block of the sublist into memory
             */
            if (Arrays.stream(block_iters).anyMatch(x -> !x.hasNext()) ){
                int[] empty_blocks = IntStream.range(0, sublists.size())
                        .filter(x -> !block_iters[x].hasNext()).toArray();
                //System.out.println("dfskajhfkjsdahfkjsdhf");
                for (int j = 0; j < empty_blocks.length; j++) {
                    int k = empty_blocks[j];

                    // if the last block of a sublist is already used, do nothing
                    if (iters[k].hasNext()){
                        bm.release(first_blocks[k], false);
                        first_blocks[k] = iters[k].next();
                        bm.load(first_blocks[k]);
                        block_iters[k] = first_blocks[k].iterator();
                    }
                }
            }

            // get smallest element of the first elements of each loaded block
            first_tuple = Collections.min(al, cd.getColumnComparator(sort_index));
            int id_smallest = al.indexOf(first_tuple);
            if (block_iters[id_smallest].hasNext())
                al.add(id_smallest, block_iters[id_smallest].next());
            al.remove(first_tuple);

            System.out.println(first_tuple);

            // write smallest element to output, and if output block ist full, then release outputblock
            out_block.append(first_tuple);
            if (out_block.isFull()){
                output.output(out_block);
                bm.release(out_block, false);
                out_block = bm.allocate(true);
            }

        }
        // release all blocks if TPMMS is finished
        bm.release(out_block, false);
        for (int i = 0; i < first_blocks.length; i++) {
            bm.release(first_blocks[i], false);
        }
        //throw new UnsupportedOperationException("TODO");
    }

}


