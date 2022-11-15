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

        return 4 * relation.getEstimatedSize();
        //throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void sort(@NotNull Relation relation, @NotNull BlockOutput output) {
        BlockManager bm = getBlockManager();
        Iterator<Block> r_it_test = relation.iterator();

        if (relation.getEstimatedSize() > bm.getFreeBlocks() * bm.getFreeBlocks())
            throw new RelationSizeExceedsCapacityException();

        int r_size = relation.getEstimatedSize();
        //System.out.println(r_size);

        ArrayList<ArrayList<Block> > sublists = new ArrayList<>((int) Math.ceil(r_size/bm.getFreeBlocks()));
        int current_list = 0;
        ArrayList<Block> blocks = new ArrayList<>();
        ColumnDefinition cd = relation.getColumns();
        int sort_index = getSortColumnIndex();
        int block_cap = 0;

        for (Iterator<Block> r_it = relation.iterator(); r_it.hasNext();) {
            Block b = r_it.next();
            bm.load(b);
            block_cap = b.getCapacity();
            blocks.add(b);
            //System.out.println(bm.getUsedBlocks());
            if (bm.getFreeBlocks() == 0){
                BlockSorter.INSTANCE.sort(relation, blocks, cd.getColumnComparator(sort_index));
                System.out.println(blocks);
                sublists.add(current_list, new ArrayList<>());
                for (Block block:
                        blocks) {
                    //output.output(b);
                    sublists.get(current_list).add(block);
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
        Iterator<Block>[] iters = new Iterator[sublists.size()];
//        System.out.println(iters);
//        Iterator test = sublists.get(0).iterator();
//        Block b = (Block) test.next();
//        b = (Block) test.next();
//        b = (Block) test.next();
//        bm.load(b);
//        System.out.println(b);
        for (int i = 0; i < sublists.size(); i++) {
            iters[i] = sublists.get(i).iterator();
        }
        //System.out.println(iters[0].next());

        Block out_block = bm.allocate(true);
        Block[] first_blocks = new Block[sublists.size()];
        Iterator<Tuple>[] block_iters = new Iterator[sublists.size()];
        PriorityQueue<Tuple> pq = new PriorityQueue<>(sublists.size(),
                cd.getColumnComparator(sort_index));
        Tuple first_tuple;


        if (Arrays.stream(first_blocks).anyMatch(x -> Objects.isNull(x))){
            // List<Block> empty_blocks = Arrays.stream(first_blocks).filter(x -> x.isEmpty()).collect(Collectors.toList());
            int[] empty_blocks = IntStream.range(0, sublists.size())
                    .filter(x -> first_blocks[x] == null).toArray();
            //System.out.println(empty_blocks.length);
            for (int j = 0; j < empty_blocks.length; j++) {
                int k = empty_blocks[j];
                first_blocks[k] = iters[k].next();
                bm.load(first_blocks[k]);
                block_iters[k] = first_blocks[k].iterator();

                Tuple test = block_iters[k].next();
                System.out.println(test);
                pq.add(test);
                //System.out.println(pq.peek());
            }
        }
        System.out.println();
        System.out.println(block_cap);
        System.out.println();
        for (int i = 0; i < r_size*block_cap; i++) {

            if (Arrays.stream(block_iters).anyMatch(x -> !x.hasNext())){
                // List<Block> empty_blocks = Arrays.stream(first_blocks).filter(x -> x.isEmpty()).collect(Collectors.toList());
                int[] empty_blocks = IntStream.range(0, sublists.size())
                        .filter(x -> !block_iters[x].hasNext()).toArray();
                System.out.println("dfskajhfkjsdahfkjsdhf");
                for (int j = 0; j < empty_blocks.length; j++) {
                    int k = empty_blocks[j];
                    bm.release(first_blocks[k], false);
                    first_blocks[k] = iters[k].next();
                    bm.load(first_blocks[k]);
                    block_iters[k] = first_blocks[k].iterator();
                    pq.add(block_iters[k].next());
                    //System.out.println(pq.peek());
                }
            }


            first_tuple = pq.poll();
            //System.out.println(first_blocks[0]);
            //System.out.println(first_blocks[1]);
            System.out.println(first_tuple);
            for (int j = 0; j < block_iters.length; j++) {
                if (contains(block_iters[j], first_tuple))
                    pq.add(block_iters[i].next());
            }



            out_block.append(first_tuple);
            if (out_block.isFull()){
                output.output(out_block);
                bm.release(out_block, false);
                out_block = bm.allocate(true);
            }
            //System.out.println(block_iters[0].hasNext());
            //System.out.println(block_iters[1].hasNext());

        }

        //throw new UnsupportedOperationException("TODO");
    }
}


