package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Override
    public Result sendCode(String phone, HttpSession session) {
        if(RegexUtils.isPhoneInvalid(phone)){ //注意该函数返回true意味着手机号格式错误
            return Result.fail("手机号格式错误！请重新输入");
        }
        String code = RandomUtil.randomNumbers(6);
        session.setAttribute(SystemConstants.SESSION_KEY_CACHE_CODE, code);
        log.info("发送短信验证码成功,验证码：{}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误！请重新输入");
        }
        Object cacheCode = session.getAttribute(SystemConstants.SESSION_KEY_CACHE_CODE);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.toString().equals(code)){
            return Result.fail("验证码错误");
        }
        User user = query().eq("phone", phone).one();
        if (user == null){
            user = createUserWithPhone(phone);
        }
        session.setAttribute(SystemConstants.SESSION_KEY_USER, user);
        return Result.ok();
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
