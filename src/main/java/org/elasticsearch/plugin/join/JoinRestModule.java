package org.elasticsearch.plugin.join;

import org.elasticsearch.common.inject.AbstractModule;

public class JoinRestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JoinRestHandler.class).asEagerSingleton();
    }
}