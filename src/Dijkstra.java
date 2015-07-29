public class Dijkstra {
    private static final String unknwon_nodes_count = "select count(*) from unknown_nodes";

    private static final String pivot_min_distance = "select distance  where node=pivot_node";

    private static final String drop_bfs_table = "drop table if exists tmp_distance_a";
    private static final String calc_bfs_table = "create table tmp_distance_a as select f1.dst, distance+ %d from (select distinct meta_distances.* from meta_distances as f1, unknown_nodes as f2 where f1.dst=f2.node and f1.src=%d )as tmp";

    private static final String update_min_distance = "insert overwrite table unknown_nodes select f2.node IF(f1.node is null, f2.distance, IF(f1.distance>f2.distance, f2.distance, f1.distance ) ) from tmp_distance_a as f1 right outer join unknown_nodes as f2 on f1.dst=f2.node";

    private static final String insert_min_distance_node = "insert into table known_nodes select node, distance  from unknown_nodes where distance=min(distance)";

    private static final String min_distance_node = "select node from unknown_nodes where distance=min(distance)";

    private static final String del_min_distance_node = "insert overwrite into unknown_nodes select * from unkown_nodes where distance!=min(distance)";

    public static void compute(int pivotNode) {
        HiveClient hive = new HiveClient();
        int unknownNodesCount = 1;
        int pivot_node = 1;
        int min_distance = 0;
        while (true) {
            // 更新未知最短距离的点数量
            unknownNodesCount = hive.get(unknwon_nodes_count);
            if (unknownNodesCount <= 0) {
                // 所有点的最短距离都已经计算出
                break;
            }
            // 更新最短距离
            min_distance = hive.get(pivot_min_distance);
            // 计算unknown_nodes中每一个node若经过pivot_node，与源点的距离
            hive.execute(drop_bfs_table);
            hive.execute(String
                    .format(calc_bfs_table, min_distance, pivot_node));

            // 更新unknown_nodes的距离信息
            hive.execute(update_min_distance);

            // 挑选出最小的node，放入known_nodes中
            hive.execute(insert_min_distance_node);

            // 挑选出最小的node，最为下一个pivot_node
            pivotNode = hive.get(min_distance_node);
            // 从unknown_nodes中删除最小node
            hive.execute(del_min_distance_node);
        }
    }
}