/**
 * Created with IntelliJ IDEA.
 * User: gongdaoqi
 * Date: 14-9-17
 * Time: 下午2:16
 * To change this template use File | Settings | File Templates.
 */
@Service
public class TmpProcessPoiMenuWithGroupPoiTask extends AbstractTask{

    private static final Logger log = LoggerFactory.getLogger(TmpProcessPoiMenuWithGroupPoiTask.class);

    @Resource
    private DataSource dataSource;

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Resource
    private PoiMergeGroupCache poiMergeGroupCache;

    @Resource
    private PoiMenuMapper poiMenuMapper;

    @Resource
    private MQMessageService mqMessageService;

    @Override
    protected void execute() throws Throwable {
        /**
         * 任务目的：将有合并组的POI的菜品同步到组内其它POI上
         * 任务流程：获取所有非主门店POI->逐个处理POI等价组->多线程并行处理等价组List
         */

        log.info("Poi合并组复制任务stage 1：获取POI等价组<<<<<");
        List<Long> allPoiIds = findPoiIdList();
        List<List<Long>> shards = PoiUtil.shardListByPerNum(allPoiIds,1000);
        log.info("Poi合并组复制任务stage 1：获取POI等价组>>>>>完成");

        log.info("Poi合并组复制任务stage 2：多线程处理POI等价组<<<<<");

        CountDownLatch countDownLatch = new CountDownLatch(shards.size());

        for(int i=0;i<shards.size();i++){
            ProcessPoiMenuWithGroupPoiRunner processPoiMenuWithGroupPoiRunner = new ProcessPoiMenuWithGroupPoiRunner();
            processPoiMenuWithGroupPoiRunner.setParam(shards.get(i),i,countDownLatch);
            executorService.execute(processPoiMenuWithGroupPoiRunner);
        }

        executorService.shutdown();
        log.info("Poi合并组复制任务stage 2：多线程处理POI等价组>>>>完成");
        countDownLatch.await();
        log.info("任务完成！");

    }


    public class ProcessPoiMenuWithGroupPoiRunner implements Runnable{

        private List<Long> poiIds;
        private CountDownLatch countDownLatch;
        private int offset;

        public void setParam(List<Long> ids,int offset,CountDownLatch countDownLatch) {
            this.poiIds = ids;
            this.offset = offset;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            if(poiIds!=null && poiIds.size()>0){
                log.info("Poi合并组复制任务：当前第" + offset + "K条");
                Map<Long,List<Long>> mergeGroupMap = poiMergeGroupCache.getMapByKeys(poiIds);
                for(Iterator iterator = mergeGroupMap.values().iterator(); iterator.hasNext();){
                    Object obj = iterator.next();
                    processingMergeGroup((List<Long>) obj);
                }
                log.info("Poi合并组复制任务：当前第" + offset + "K条done，当前list.size() = " + poiIds.size());
            }
            countDownLatch.countDown();
        }
    }

    //处理一个等价组的poi
    public void processingMergeGroup(List<Long> ids){

        //按照POIID->POIMENUS返回map
        Map<Long,List<PoiMenu>> menuMap = new HashMap<>();
        List<PoiMenu> menus = poiMenuMapper.findByPoiIds(ids,null);
        if(menus==null || menus.size()==0){
            return ;
        }
        for (PoiMenu menu : menus) {
            if (menuMap.containsKey(menu.getPoiId())) {
                menuMap.get(menu.getPoiId()).add(menu);
            } else {
                List<PoiMenu> list = new ArrayList<PoiMenu>();
                list.add(menu);
                menuMap.put(menu.getPoiId(), list);
            }
        }

        //记录每个poi的菜品数map
        Map<Long,Integer> countMap = new HashMap<>();
        for(Long id:ids){
            countMap.put(id,0);
        }
        for(Iterator iterator = menuMap.keySet().iterator(); iterator.hasNext();){
            Object obj = iterator.next();
            countMap.put((Long) obj,menuMap.get(obj).size());
        }

        //记录需要复制的poi和被复制的poi
        Long maxId = 0l;
        int maxCount = 0;
        List<Long> needCopyIds = new ArrayList<>();
        for(Iterator iterator = countMap.keySet().iterator(); iterator.hasNext();){
            Object obj = iterator.next();
            int count = countMap.get(obj);
            if(count>maxCount){
                maxCount = count;
                maxId = (Long)obj;
            }
            if(count==0){
                needCopyIds.add((Long) obj);
            }
        }

        if(needCopyIds.size()==0){
            return ;
        }

        for(Long poiId:needCopyIds){
            List<PoiMenu> copyMenus = menuMap.get(maxId);
            for(PoiMenu item : copyMenus){
                item.setPoiId(poiId);
            }
            poiMenuMapper.insertBatch(copyMenus);
            mqMessageService.sendInternalMessage(InternalMessageFactory.createPoiDealSumCountMessage(poiId));
            //stagePoiCacheListener.onListen(poiId);
        }

    }

    public List<Long> findPoiIdList(){
        List<Long> poiIds = new ArrayList<>();
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            String sql = "select distinct merge_id from meituanpoiop.stage_poi where merge_id > 0";
            ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                poiIds.add(rs.getLong(1));
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            log.info(e.getMessage(), e);
        } finally {
            try {
                connection.close();
            } catch (SQLException ex) {
                log.info(ex.getMessage(), ex);
            }
        }
        return poiIds;
    }

}
package com.sankuai.meituan.poiop.task.tmp;

import com.sankuai.meituan.poiop.cache.impl.PoiMergeGroupCache;
import com.sankuai.meituan.poiop.cache.listener.StagePoiCacheListener;
import com.sankuai.meituan.poiop.dao.mapper.PoiMenuMapper;
import com.sankuai.meituan.poiop.domain.PoiMenu;
import com.sankuai.meituan.poiop.mq.InternalMessageFactory;
import com.sankuai.meituan.poiop.service.impl.MQMessageService;
import com.sankuai.meituan.poiop.util.PoiUtil;
import com.sankuai.meituan.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: gongdaoqi
 * Date: 14-9-17
 * Time: 下午2:16
 * To change this template use File | Settings | File Templates.
 */
@Service
public class TmpProcessPoiMenuWithGroupPoiTask extends AbstractTask{

    private static final Logger log = LoggerFactory.getLogger(TmpProcessPoiMenuWithGroupPoiTask.class);

    @Resource
    private DataSource dataSource;

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Resource
    private StagePoiCacheListener stagePoiCacheListener;

    @Resource
    private PoiMergeGroupCache poiMergeGroupCache;

    @Resource
    private PoiMenuMapper poiMenuMapper;

    @Resource
    private MQMessageService mqMessageService;

    @Override
    protected void execute() throws Throwable {
        /**
         * 任务目的：将有合并组的POI的菜品同步到组内其它POI上
         * 任务流程：获取所有非主门店POI->逐个处理POI等价组->多线程并行处理等价组List
         */

        log.info("Poi合并组复制任务stage 1：获取POI等价组<<<<<");
        List<Long> allPoiIds = findPoiIdList();
        List<List<Long>> shards = PoiUtil.shardListByPerNum(allPoiIds,1000);
        log.info("Poi合并组复制任务stage 1：获取POI等价组>>>>>完成");

        log.info("Poi合并组复制任务stage 2：多线程处理POI等价组<<<<<");

        CountDownLatch countDownLatch = new CountDownLatch(shards.size());

        for(int i=0;i<shards.size();i++){
            ProcessPoiMenuWithGroupPoiRunner processPoiMenuWithGroupPoiRunner = new ProcessPoiMenuWithGroupPoiRunner();
            processPoiMenuWithGroupPoiRunner.setParam(shards.get(i),i,countDownLatch);
            executorService.execute(processPoiMenuWithGroupPoiRunner);
        }

        executorService.shutdown();
        log.info("Poi合并组复制任务stage 2：多线程处理POI等价组>>>>完成");
        countDownLatch.await();
        log.info("任务完成！");

    }


    public class ProcessPoiMenuWithGroupPoiRunner implements Runnable{

        private List<Long> poiIds;
        private CountDownLatch countDownLatch;
        private int offset;

        public void setParam(List<Long> ids,int offset,CountDownLatch countDownLatch) {
            this.poiIds = ids;
            this.offset = offset;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            if(poiIds!=null && poiIds.size()>0){
                log.info("Poi合并组复制任务：当前第" + offset + "K条");
                Map<Long,List<Long>> mergeGroupMap = poiMergeGroupCache.getMapByKeys(poiIds);
                for(Iterator iterator = mergeGroupMap.values().iterator(); iterator.hasNext();){
                    Object obj = iterator.next();
                    processingMergeGroup((List<Long>) obj);
                }
                log.info("Poi合并组复制任务：当前第" + offset + "K条done，当前list.size() = " + poiIds.size());
            }
            countDownLatch.countDown();
        }
    }

    //处理一个等价组的poi
    public void processingMergeGroup(List<Long> ids){

        //按照POIID->POIMENUS返回map
        Map<Long,List<PoiMenu>> menuMap = new HashMap<>();
        List<PoiMenu> menus = poiMenuMapper.findByPoiIds(ids,null);
        if(menus==null || menus.size()==0){
            return ;
        }
        for (PoiMenu menu : menus) {
            if (menuMap.containsKey(menu.getPoiId())) {
                menuMap.get(menu.getPoiId()).add(menu);
            } else {
                List<PoiMenu> list = new ArrayList<PoiMenu>();
                list.add(menu);
                menuMap.put(menu.getPoiId(), list);
            }
        }

        //记录每个poi的菜品数map
        Map<Long,Integer> countMap = new HashMap<>();
        for(Long id:ids){
            countMap.put(id,0);
        }
        for(Iterator iterator = menuMap.keySet().iterator(); iterator.hasNext();){
            Object obj = iterator.next();
            countMap.put((Long) obj,menuMap.get(obj).size());
        }

        //记录需要复制的poi和被复制的poi
        Long maxId = 0l;
        int maxCount = 0;
        List<Long> needCopyIds = new ArrayList<>();
        for(Iterator iterator = countMap.keySet().iterator(); iterator.hasNext();){
            Object obj = iterator.next();
            int count = countMap.get(obj);
            if(count>maxCount){
                maxCount = count;
                maxId = (Long)obj;
            }
            if(count==0){
                needCopyIds.add((Long) obj);
            }
        }

        if(needCopyIds.size()==0){
            return ;
        }

        for(Long poiId:needCopyIds){
            List<PoiMenu> copyMenus = menuMap.get(maxId);
            for(PoiMenu item : copyMenus){
                item.setPoiId(poiId);
            }
            poiMenuMapper.insertBatch(copyMenus);
            mqMessageService.sendInternalMessage(InternalMessageFactory.createPoiDealSumCountMessage(poiId));
            //stagePoiCacheListener.onListen(poiId);
        }

    }

    public List<Long> findPoiIdList(){
        List<Long> poiIds = new ArrayList<>();
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            String sql = "select distinct merge_id from meituanpoiop.stage_poi where merge_id > 0";
            ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                poiIds.add(rs.getLong(1));
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            log.info(e.getMessage(), e);
        } finally {
            try {
                connection.close();
            } catch (SQLException ex) {
                log.info(ex.getMessage(), ex);
            }
        }
        return poiIds;
    }

}
