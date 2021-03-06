/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hedwig.admin.console;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.hedwig.admin.HedwigAdmin;

import jline.Completor;

import static org.apache.hedwig.admin.console.HedwigCommands.*;

/**
 * A jline completor for hedwig console
 */
public class JLineHedwigCompletor implements Completor {
    private HedwigAdmin admin;

    public JLineHedwigCompletor(HedwigAdmin admin) {
        this.admin = admin;
    }

    @Override
    public int complete(String buffer, int cursor, List candidates) {
        // Guarantee that the final token is the one we're expanding
        buffer = buffer.substring(0,cursor);
        String[] tokens = buffer.split(" ");
        if (buffer.endsWith(" ")) {
            String[] newTokens = new String[tokens.length + 1];
            System.arraycopy(tokens, 0, newTokens, 0, tokens.length);
            newTokens[newTokens.length - 1] = "";
            tokens = newTokens;
        }
        
        if (tokens.length > 2 &&
            DESCRIBE.equalsIgnoreCase(tokens[0]) &&
            DESCRIBE_TOPIC.equalsIgnoreCase(tokens[1])) {
            return completeTopic(buffer, tokens[2], candidates);
        } else if (tokens.length > 1 &&
                   (SUB.equalsIgnoreCase(tokens[0]) ||
                    PUB.equalsIgnoreCase(tokens[0]) ||
                    CLOSESUB.equalsIgnoreCase(tokens[0]) ||
                    CONSUME.equalsIgnoreCase(tokens[0]) ||
                    CONSUMETO.equalsIgnoreCase(tokens[0]) ||
                    READTOPIC.equalsIgnoreCase(tokens[0]))) {
            return completeTopic(buffer, tokens[1], candidates);
        }
        List<String> cmds = HedwigCommands.findCandidateCommands(tokens);
        return completeCommand(buffer, tokens[tokens.length - 1], cmds, candidates);
    }

    private int completeCommand(String buffer, String token,
            List<String> commands, List<String> candidates) {
        for (String cmd : commands) {
            if (cmd.startsWith(token)) {
                candidates.add(cmd);
            }
        }
        return buffer.lastIndexOf(" ") + 1;
    }

    private int completeTopic(String buffer, String token, List<String> candidates) {
        try {
            List<String> children = admin.getTopics();
            for (String child : children) {
                if (child.startsWith(token)) {
                    candidates.add(child);
                }
            }
        } catch (Exception e) {
            return buffer.length();
        }
        return candidates.size() == 0 ? buffer.length() : buffer.lastIndexOf(" ") + 1;
    }
}
