package com.alibaba.cobar.route.function;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:dragon829@gmail.com">lostdragon</a>
 */
public class PartitionByMod extends FunctionExpression {
    public PartitionByMod(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    protected int count;

    public void setPartitionCount(String partitionCount) {
        this.count = Integer.parseInt(partitionCount);
    }

    protected int partitionIndex(long hash) {
        return (int)(hash % count);
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        Object arg = arguments.get(0).evaluation(parameters);
        if (arg == null) {
            throw new IllegalArgumentException("partition key is null ");
        } else if (arg == UNEVALUATABLE) {
            throw new IllegalArgumentException("argument is UNEVALUATABLE");
        }
        Number key;
        if (arg instanceof Number) {
            key = (Number) arg;
        } else if (arg instanceof String) {
            key = Long.parseLong((String) arg);
        } else {
            throw new IllegalArgumentException("unsupported data type for partition key: " + arg.getClass());
        }
        return partitionIndex(key.longValue());
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        if (arguments == null || arguments.size() != 1)
            throw new IllegalArgumentException("function "
                    + getFunctionName()
                    + " must have 1 argument but is "
                    + arguments);
        PartitionByMod partitionFunc = new PartitionByMod(functionName, arguments);
        partitionFunc.count = count;
        return partitionFunc;
    }
}
