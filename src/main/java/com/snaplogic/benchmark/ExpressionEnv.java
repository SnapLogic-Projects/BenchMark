package com.snaplogic.benchmark;

import com.snaplogic.common.expressions.ScopeStack;
import com.snaplogic.expression.*;
import java.util.HashMap;
import java.util.Map;

public class ExpressionEnv {

    private HashMap<String, Object> jsonData;
    private HashMap<String, Object> envParam;

    public ExpressionEnv(){
    }

    public ExpressionEnv(Map<String, Object> jsonData) {

        this.jsonData = new HashMap<String, Object>(jsonData);
        this.envParam = new HashMap<String, Object>();
    }

    static public ScopeStack InitializeEnvData(Map<String, Object> envData) {
        ScopeStack scopeStack = new ScopeStack();
        scopeStack.push(new GlobalScope());
        if (envData != null) {
            scopeStack.push(new EnvironmentScope(envData));
        }
        return scopeStack;
    }
}
