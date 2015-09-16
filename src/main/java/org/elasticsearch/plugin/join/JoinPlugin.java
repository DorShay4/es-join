package org.elasticsearch.plugin.join;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class JoinPlugin extends AbstractPlugin {
	
	public String name() {
        return "join-plugin";
    }

    public String description() {
        return "geoJoin and Join";
    }
    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(JoinRestModule.class);
        return modules;
    }
}
