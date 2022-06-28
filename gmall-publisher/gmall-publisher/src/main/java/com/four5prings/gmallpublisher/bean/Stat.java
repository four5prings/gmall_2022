package com.four5prings.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Stat
 * @Description
 * @Author Four5prings
 * @Date 2022/6/28 16:25
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class Stat {
     String title;
     List<Option> options;

}

