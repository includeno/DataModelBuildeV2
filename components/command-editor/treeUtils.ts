import { OperationNode } from '../../types';

export const flattenNodes = (root: OperationNode): OperationNode[] => {
    let result = [root];
    if (root.children) {
        root.children.forEach(child => {
            result = [...result, ...flattenNodes(child)];
        });
    }
    return result;
};

export const findAncestorVariables = (root: OperationNode, currentId: string): string[] => {
    const vars: string[] = [];
    const traverse = (node: OperationNode): boolean => {
        if (node.id === currentId) return true; 
        let foundInChild = false;
        if (node.children) {
            for (const child of node.children) {
                if (traverse(child)) {
                    foundInChild = true;
                    break;
                }
            }
        }
        if (foundInChild) {
            node.commands.forEach(cmd => {
                if (cmd.type === 'save' && cmd.config.value) {
                    vars.push(String(cmd.config.value));
                }
                if (cmd.type === 'define_variable' && cmd.config.variableName) {
                    vars.push(cmd.config.variableName);
                }
            });
            return true;
        }
        return false;
    };
    if (root) traverse(root);
    return vars;
};

export const getAncestors = (node: OperationNode, targetId: string): OperationNode[] | null => {
    if (node.id === targetId) return [];
    if (node.children) {
        for (const child of node.children) {
            const res = getAncestors(child, targetId);
            if (res) return [node, ...res];
        }
    }
    return null;
};
