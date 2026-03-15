import React from 'react';
import { Reorder, useDragControls, DragControls } from 'framer-motion';
import { Command } from '../../types';

export const DraggableItem = ({ cmd, children }: { cmd: Command; children: (dragControls: DragControls) => React.ReactNode }) => {
  const dragControls = useDragControls();
  return (
    <Reorder.Item
      value={cmd}
      dragListener={false}
      dragControls={dragControls}
      as="div"
      className="relative"
    >
      {children(dragControls)}
    </Reorder.Item>
  );
};
