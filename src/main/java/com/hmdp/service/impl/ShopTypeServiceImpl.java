package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IShopTypeService typeService;

    @Override
    public Result queryByTypeList() {
        //查询redis，查到直接返回
        String typeListJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE);
        if(StrUtil.isNotBlank(typeListJson)){
            return Result.ok(JSONUtil.parseArray(typeListJson).toList(ShopType.class));
        }

        //没有查到则查数据库
        List<ShopType> typeList = typeService
                .query().orderByAsc("sort").list();
        if(typeList.size()==0){
            return Result.fail("商铺分类不存在");
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
