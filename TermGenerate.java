package com.sankuai.meituan.poiop.similar.calculator;

import com.sankuai.meituan.poiop.similar.TermFreInformationVo;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: gongdaoqi
 * Date: 14-7-16
 * Time: 下午7:55
 * To change this template use File | Settings | File Templates.
 */
public class TermGenerate{

    public static Integer maxTermLength = 6;

    //建立后缀数组
    static List<String> getSuffixArray(String str){
        List<String> resultList = new ArrayList<>();
        int last = str.length();
        for(int i=0;i<last;i++){
            String subStr =  str.substring(i,last);
            resultList.add(subStr);
        }
        return resultList;
    }

    //建立词频Map
    static void getTermCountMap(String str,Map<String,Integer> termCountMap){
        String term = "";
        for(int i=0;i<=maxTermLength;i++){
            for(int j=0;j<str.length()-i;j++){
                term = str.substring(j,j+i+1);
                if(termCountMap.get(term)!=null){
                    termCountMap.put(term,termCountMap.get(term)+1);
                }else{
                    termCountMap.put(term,1);
                }
            }
        }
    }

    //计算信息熵
    static float getInformationQuantity(List<Character> chars){
        //找出左邻字
        Map<Character,Integer> leftWordMap = new HashMap<>();
        String leftWord = "";
        for(Character ch:chars){
            if(leftWordMap.get(ch)!=null){
                leftWordMap.put(ch,leftWordMap.get(ch)+1);
            }else{
                leftWordMap.put(ch,1);
            }
        }

        if(leftWordMap.size()==0){
            return -1f;
        }

        //信息熵统计方法
        float result = 0;
        Integer count = chars.size();
        for (Iterator i = leftWordMap.keySet().iterator(); i.hasNext();) {
            Object obj = i.next();
            float num = (float)leftWordMap.get(obj)/count;
            result = (float) (result - (Math.log(num))*num);
        }

        return result;

    }

    //在有序后缀数组中查找以str开头的字符串的下一个字符list
    public static List<Character> getSuffixWordList(List<String> array, String str){

        List<Character> resultList = new ArrayList<>();

        int index = binarySearch(array,str);

        if(index>-1){

            for(int i=index;i<array.size() && array.get(i).indexOf(str)==0;i++){
                if(array.get(i).length()>str.length()){
                    Character suffix = array.get(i).charAt(str.length());
                    resultList.add(suffix);
                }
            }

            for(int i=index-1;i>=0 && array.get(i).indexOf(str)==0;i--){
                if(array.get(i).length()>str.length()){
                    Character suffix = array.get(i).charAt(str.length());
                    resultList.add(suffix);
                }
            }
        }
        return resultList;

    }

    //二分查找有序数组，返回字符串所在的位置
    public static Integer binarySearch(List<String> array, String str){

        int left=0, right=array.size()-1;
        String curStr;
        while(left<=right){
            int middle = (left+right)>>>1;
            curStr = array.get(middle);

            if(compareWithPrefix(curStr, str)<0){
                left = middle + 1;
            }else if(compareWithPrefix(curStr, str)>0)
            {
                right = middle - 1;
            }else{
                return middle;
            }

        }
        return -1;
    }

    //查找连接词
    public static int compareWithPrefix(String str,String compStr){
        if(str.length() <= compStr.length()){
            return -compStr.compareTo(str);
        }else{
            return -compStr.compareTo(str.substring(0,compStr.length()));
        }
    }

    public static void processSingle(String str,List<String> suffixArray,List<String> prefixArray,Map<String,Integer> termCountMap, BufferedWriter bw) throws IOException {

        //计算左信息熵
        List<Character> leftWord = getSuffixWordList(suffixArray,str);
        float leftInformationQuantity =  getInformationQuantity(leftWord);

        List<Character> rightWord = getSuffixWordList(prefixArray,new StringBuffer(str).reverse().toString());
        float rightInformationQuantity = getInformationQuantity(rightWord);

        bw.write(str + "\t" + termCountMap.get(str) + "\t" + leftInformationQuantity + "\t" + rightInformationQuantity + "\n");

    }

    //处理名称，去掉末尾的（）和()等
    public static void getPointName() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/Users/gongdaoqi/pois.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/gongdaoqi/pointName.txt"));
        String line="";
        String pointName = "";
        List<String> names = new ArrayList<>();
        while((line = br.readLine())!=null){
            String[] lines = line.split("\t");
            pointName = lines[0];
            if(pointName.length()>10){
                continue;
            }
           // if(pointName.matches("[a-zA-Z]")){

            if(pointName.matches("(.*)[a-zA-Z]{1,}(.*)")){
                continue;
            }
            pointName = pointName.replaceAll("（[^）]*）", "");
            pointName = pointName.replaceAll("\\([^\\)]*\\)", "");
            pointName = pointName.replaceAll("\\s","");
            //去除特殊符号
            bw.write(pointName + "\n");
        }
        bw.flush();
    }

    //获取所有潜在词
    public static void getPointNameWordSet() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/Users/gongdaoqi/pointName.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/gongdaoqi/pointNameWordSet.txt"));
        String line="";
        String pointName = "";
        Set<String> wordSet = new HashSet<>();
        List<String> names = new ArrayList<>();
        while((line = br.readLine())!=null){
            String[] lines = line.split("\t");
            pointName = lines[0];
            for(int i=2;i<=maxTermLength;i++){
                for(int j=0;j<pointName.length()-i;j++){
                    wordSet.add(pointName.substring(j,j+i));
                }
            }
        }

        List<String> words =  new ArrayList<>(wordSet);

        for(String word : words){
            bw.write(word+"\n");
        }

        bw.flush();
    }

    //计算凝聚程度
    public static float getCohesionValue(String str,Map<String,Integer> termCount){

        //防止溢出
        BigDecimal mainCount = BigDecimal.valueOf(termCount.get(str));
        BigDecimal result = BigDecimal.ONE;
        BigDecimal count1,count2;
        BigDecimal frequency;

        for(int i=1;i<str.length();i++){
            count1 = BigDecimal.valueOf(termCount.get(str.substring(0, i)));
            count2 = BigDecimal.valueOf(termCount.get(str.substring(i,str.length())));
            frequency = mainCount.divide(count1.multiply(count2),10,BigDecimal.ROUND_HALF_UP);
            if(frequency.compareTo(result)<0){
                result = frequency;
            }
        }
        return result.floatValue();
    }

    /**
     * 获取品牌词，获取思路
     * 1.计算词频、右邻信息熵、凝聚程度
     * 2.对pointName进行正向切词，并逐条记录
     */
    public static void getBrandName(List<String> suffixArray, Map<String,Integer> termCountMap) throws IOException {

        //假如某潜在词出现在poi名称开头
        BufferedReader br = new BufferedReader(new FileReader("/Users/gongdaoqi/pointNameNotRe.txt"));
        BufferedWriter bw1 = new BufferedWriter(new FileWriter("/Users/gongdaoqi/brand.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/gongdaoqi/brand_detail.txt"));

        String line = "";
        String word = "";
        String matchStr = "";

        List<Character> rightWord = new ArrayList<>();
        List<TermFreInformationVo> termVos = new ArrayList<>();

        Float cohensive,rightInformationValue;

        Set<TermFreInformationVo> bigBrands = new HashSet<>();

        while((line=br.readLine())!=null){
            if(line.length()>10){
                continue;
            }
            for(int i=2;i<=line.length() && i<=maxTermLength;i++){

                word = line.substring(0, i);
                //右邻信息熵
                rightWord = getSuffixWordList(suffixArray,word);
                rightInformationValue =  getInformationQuantity(rightWord);
                cohensive = getCohesionValue(word, termCountMap);

                bw1.write(word + "\t" + termCountMap.get(word)
                + "\t" + String.format("%1$.10f",cohensive) + "\t" +
                rightInformationValue + "\n");

                if(termCountMap.get(word).equals(1)){
                    continue;
                }


                if(cohensive.compareTo(0.000001f)<0){
                    continue;
                }

                TermFreInformationVo vo = new TermFreInformationVo();

                vo.setTerm(word);

                if(rightInformationValue.compareTo(0.1f)<0){
                    continue;
                }
                vo.setRightInformationValue(rightInformationValue);
                vo.setTermCount(termCountMap.get(word));
                vo.setCohensiveValue(getCohesionValue(word, termCountMap));

                termVos.add(vo);

            }
            if(termVos.size()>1){
                matchStr = getBigBrand(termVos);
                if(matchStr!=""){
                    int index = line.indexOf(matchStr);
                    bw.write(matchStr + "\t" +line.substring(matchStr.length(),line.length()) + "\n");
                }else{
                    bw.write(line + "\n");
                }
            }else{
                bw.write(line + "\n");
            }

            termVos.clear();

        }

//        for (TermFreInformationVo vo : bigBrands) {
//            bw.write(vo.toString());
//        }
        bw.flush();
        bw1.flush();

    }

    public static String getBigBrand(List<TermFreInformationVo> vos){

        if(vos==null||vos.size()==0){
            return "";
        }
        if(vos.size()==1){
            return vos.get(0).getTerm();
        }

        int index = 0;
        Float max = 0f;
        Float curScore = 0f;
        for(int i=0;i<vos.size();i++){
            curScore = (vos.get(i).getCohensiveValue())*(vos.get(i).getTermCount());
            if(curScore.compareTo(max)>0){
                max = curScore;
                index = i;
            }
        }

        return vos.get(index).getTerm();

    }

    public static void main(String[] args) throws IOException {


        getPointName();
        getPointNameWordSet();


        BufferedReader br = new BufferedReader(new FileReader("/Users/gongdaoqi/pointName.txt"));

        List<String> suffixArray = new ArrayList<>();
        List<String> prefixArray = new ArrayList<>();
        Map<String,Integer> termCountMap = new HashMap<>();

        String line = "";
        int count=0;
        int sumCount = 0;
        while((line=br.readLine())!=null){
            if(line.length()>10){
                continue;
            }
            suffixArray.addAll(getSuffixArray(line));
            prefixArray.addAll(getSuffixArray(new StringBuffer(line).reverse().toString()));
            getTermCountMap(line,termCountMap);
        }

        Collections.sort(suffixArray);
        Collections.sort(prefixArray);


        getBrandName(suffixArray,termCountMap);


        //计算左信息熵
        //String word = "美容";

//        String[] strs = {"鱼岛海鲜","鱼岛","鱼岛海鲜酒","巴贝拉","新辣道","汉庭","重庆刘一手"};
//
//        for(String word:strs){
//
//            List<Character> leftWord = getSuffixWordList(suffixArray,word);
//            float leftInformationQuantity =  getInformationQuantity(leftWord);
//
//            List<Character> rightWord = getSuffixWordList(prefixArray,new StringBuffer(word).reverse().toString());
//            float rightInformationQuantity = getInformationQuantity(rightWord);
//        float cohesionValue = getCohesionValue(word,termCountMap);
//
//        System.out.println(word + "\t" + termCountMap.get(word) + "\t" + String.format("%1$.10f",cohesionValue) + "\t"  + rightInformationQuantity + "\t" + leftInformationQuantity);
//        }


            //bw.write(word + "\t" + termCountMap.get(word) + "\t" + leftInformationQuantity + "\t" + rightInformationQuantity + "\n");

//        BufferedReader wordBw = new BufferedReader(new FileReader("/Users/gongdaoqi/pointNameWordSet.txt"));
//        BufferedWriter bw = new BufferedWriter(new FileWriter("/Users/gongdaoqi/term.txt"));
//
//        String word="";
//
//        while((word=wordBw.readLine())!=null){
//            //计算左信息熵
//            List<Character> leftWord = getSuffixWordList(suffixArray,word);
//            float leftInformationQuantity =  getInformationQuantity(leftWord);
//
//            List<Character> rightWord = getSuffixWordList(prefixArray,new StringBuffer(word).reverse().toString());
//            float rightInformationQuantity = getInformationQuantity(rightWord);
//
//            float cohesionValue = getCohesionValue(word,termCountMap);
//
//            bw.write(word + "\t" + termCountMap.get(word) + "\t" + String.format("%1$.10f",cohesionValue) + "\t"  + rightInformationQuantity + "\t" + leftInformationQuantity + "\t" + "\n");
//        }
//
//
//        bw.flush();




//        String testStr = "吃葡萄不吐葡萄皮不吃葡萄倒吐葡萄皮";
//
//
//
//        List<String> suffixArray = getSuffixArray(testStr);
//        List<String> prefixArray = getSuffixArray(new StringBuffer(testStr).reverse().toString());
//
//        Collections.sort(prefixArray);
//        Collections.sort(suffixArray);
//
//        Map<String,Integer> termCountMap = new HashMap<>();
//        getTermCountMap(testStr,termCountMap);
//
//        String word = "";
//
//        for(int i=2;i<=2;i++){
//            for(int j=0;j<testStr.length()-i;j++){
//                word = testStr.substring(j,j+i);
//                //计算左信息熵
//                List<Character> leftWord = getSuffixWordList(suffixArray,word);
//                float leftInformationQuantity =  getInformationQuantity(leftWord);
//                List<Character> rightWord = getSuffixWordList(prefixArray,new StringBuffer(word).reverse().toString());
//                float rightInformationQuantity = getInformationQuantity(rightWord);
//                System.out.println(word + "\t" + "left = " + leftInformationQuantity + "\t" + "right=" + rightInformationQuantity);
//            }
//        }

    }

}
