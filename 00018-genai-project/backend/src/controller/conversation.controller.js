
import { AppError } from '../utils/AppError.js';
import { sendSuccess } from '../utils/apiResponse.js';
import { generateTitle } from '../services/ai/ai-tittle.service.js';

import {
    createConversation,
    findConversationByIdAndUser,
    findConversationsByUser
} from '../DAO/conversation.dao.js';

import {
    findMessagesByConversation,
    createMessage
} from '../DAO/message.dao.js';

import model from '../services/ai/main.service.js';

import { HumanMessage, AIMessage, SystemMessage } from 'langchain';
import { MCPAdapter } from '@langchain/mcp-adapters';
import { createAgent } from 'langchain';

// MCP server configuration
const adapter = new MCPAdapter({
    servers: {
        tavily: {
            command: 'npx',
            args: ['-y', 'tavily-mcp@latest'],
            env: {
                TAVILY_API_KEY: process.env.TAVILY_API_KEY
            }
        }
    }
});

// Load MCP tools
const tools = await adapter.listTools();


/**
 * @POST
 * @route /api/v1/conversation
 * @desc Handle user message and stream AI response
 */
export async function handleMessage(req, res, next) {

    try {

        const { content, conversation: conversationId } = req.body;
        const user = req.user;

        if (!content || !content.trim()) {
            return next(new AppError(400, 'Message content is required'));
        }

        let conversation;

        // Create new conversation or fetch existing conversation
        if (!conversationId) {

            const { title } = await generateTitle(content);

            conversation = await createConversation({
                user: user.id,
                title
            });

        } else {

            conversation = await findConversationByIdAndUser(
                conversationId,
                user.id
            );

            if (!conversation) {
                return next(new AppError(404, 'Conversation not found'));
            }
        }

        // Fetch previous messages
        const previousMessages = await findMessagesByConversation(
            conversation.id
        );

        // Convert database messages to LangChain messages
        const messages = previousMessages.map(msg => {

            if (msg.role === 'user') {
                return new HumanMessage(msg.content);
            }

            if (msg.role === 'assistant') {
                return new AIMessage(msg.content);
            }

            return null;

        }).filter(Boolean);

        // Add current user message
        messages.push(new HumanMessage(content));

        // Save user message
        await createMessage({
            role: 'user',
            content,
            conversation: conversation._id
        });

        // Create AI agent
        const agent = createAgent({
            model,
            tools
        });

        // Stream AI response
        const stream = await agent.stream(
            {
                messages: [
                    new SystemMessage(
                        `Your name is Downbot.
                        You are a helpful AI assistant.
                        Answer users clearly and helpfully.`
                    ),
                    ...messages
                ]
            },
            {
                streamMode: 'messages'
            }
        );

        // SSE headers
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection', 'keep-alive');

        // Send conversation details
        res.write(`data: ${JSON.stringify({
            conversationId: conversation.id,
            title: conversation.title
        })}\n\n`);

        let fullResponse = '';

        // Stream chunks to frontend
        for await (const [chunk] of stream) {

            const chunkContent = chunk.content;

            if (!chunkContent) continue;

            let textContent = '';

            // Handle string content
            if (typeof chunkContent === 'string') {
                textContent = chunkContent;
            }

            // Handle array content blocks
            else if (Array.isArray(chunkContent)) {

                textContent = chunkContent
                    .filter(block => block.type === 'text')
                    .map(block => block.text || '')
                    .join('');
            }

            if (!textContent) continue;

            fullResponse += textContent;

            res.write(`data: ${JSON.stringify({
                chunk: textContent
            })}\n\n`);
        }

        // End stream
        res.end();

        // Save AI response
        if (fullResponse.trim()) {

            await createMessage({
                role: 'assistant',
                content: fullResponse,
                conversation: conversation._id
            });
        }

    } catch (error) {

        if (res.headersSent) {
            res.end();
            return;
        }

        next(error);
    }
}


/**
 * @GET
 * @route /api/v1/conversation/
 * @desc Get all conversations of the user
 */
export async function getAllConversations(req, res) {

    const user = req.user;

    const conversations = await findConversationsByUser(user.id);

    return sendSuccess(
        res,
        200,
        'Conversations fetched successfully',
        { conversations }
    );
}


/**
 * @GET
 * @route /api/v1/conversation/messages/:conversationId
 * @desc Get messages of a specific conversation
 */
export async function getMessagesByConversation(req, res, next) {

    const { conversationId } = req.params;
    const user = req.user;

    // Check conversation ownership
    const conversation = await findConversationByIdAndUser(
        conversationId,
        user.id
    );

    if (!conversation) {
        return next(new AppError(404, 'Conversation not found'));
    }

    // Fetch messages
    const messages = await findMessagesByConversation(conversation.id);

    return sendSuccess(
        res,
        200,
        'Messages fetched successfully',
        { messages }
    );
}
