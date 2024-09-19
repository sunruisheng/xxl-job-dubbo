package com.xxl.job.core.handler.impl;

import java.util.Map;

import com.alibaba.fastjson2.JSONObject;
import com.xxl.job.core.context.XxlJobHelper;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.rpc.service.GenericService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.handler.IJobHandler;


public class DubboJobHandler extends IJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(DubboJobHandler.class);

    /** dubbo服务的方法名 */
    private String serviceMethod;
    /** dubbo服务的方法参数类型 */
    private String[] types = new String[]{};
    /** dubbo服务的方法参数值 */
    private Object[] values = new Object[]{};


    // dubbo泛化调用
    private ReferenceConfig<GenericService> referenceConfig = new ReferenceConfig<>();


    /**
     * java.lang.String&java.lang.Integer&java.util.Map|hello world&111&{"code":"ttp"}
     * @param executorParams
     */
    private void buildParameter(String executorParams){
        String[] typeValues = executorParams.split("\\|");
        if(typeValues.length != 2){
            logger.info("dubbo server " + referenceConfig.getInterface() + "." + this.serviceMethod + " trans param error！");
            return;
        }
        if(typeValues[0].split("\\&").length != typeValues[1].split("\\&").length){
            logger.info("dubbo server " + referenceConfig.getInterface() + "." + this.serviceMethod + " trans param error！");
            return;
        }
        types = typeValues[0].split("\\&");

        values = new Object[types.length];
        String[] StringValues = typeValues[1].split("\\&");
        for(int i = 0; i < types.length; i++){
            switch (types[i]) {
                case "java.lang.String":
                    values[i] = StringValues[i];
                    break;
                case "java.lang.Integer":
                    values[i] = Integer.parseInt(StringValues[i]);
                    break;
                default:
                    values[i] = JSONObject.parseObject(StringValues[i], Map.class);
                    break;
            }

        }
    }

    public DubboJobHandler(String nacosAddress,String nacosGroup,TriggerParam triggerParam){
        //创建ApplicationConfig
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName("xxl_job");
        applicationConfig.setCheckSerializable(false);
        applicationConfig.setSerializeCheckStatus("DISABLE");
        //创建注册中心配置
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setProtocol("nacos");
        registryConfig.setAddress(nacosAddress);
        registryConfig.setGroup(nacosGroup);
        //创建服务引用配置
//        ReferenceConfig<GenericService> referenceConfig = new ReferenceConfig<>();
        //设置接口
//        referenceConfig.setInterface("com.taocares.naoms.service.IScheduleService");
        referenceConfig.setInterface(triggerParam.getExecutorHandler());
        referenceConfig.setGroup(triggerParam.getDubboGroup());
        referenceConfig.setVersion(triggerParam.getDubboVersion());
        applicationConfig.setRegistry(registryConfig);
        referenceConfig.setApplication(applicationConfig);
        //重点：设置为泛化调用
        //注：不再推荐使用参数为布尔值的setGeneric函数
        referenceConfig.setGeneric("true");
        //设置异步，不必须，根据业务而定。
        referenceConfig.setAsync(true);
        //设置超时时间
        referenceConfig.setTimeout(triggerParam.getExecutorTimeout() * 1000);
        referenceConfig.setRetries(0);

        this.serviceMethod = triggerParam.getDubboMethod();

        if(StringUtils.hasText(triggerParam.getExecutorParams())){
            buildParameter(triggerParam.getExecutorParams());
        }


    }

    @Override
    public void execute() throws Exception {

        // 用com.alibaba.dubbo.rpc.service.GenericService可以替代所有接口引用
        GenericService genericService = referenceConfig.get();
        if (genericService == null) {
            referenceConfig.destroy();
            logger.info("dubbo server " + referenceConfig.getInterface() + "." + this.serviceMethod + " is not founded！");
            return;
        }

        try {
            genericService.$invoke(this.serviceMethod, types, values);
            XxlJobHelper.handleSuccess();
        } catch (Exception e) {
            XxlJobHelper.handleFail(e.getMessage());
        }


    }
}
