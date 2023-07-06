// ExecTrace
[
    {
        $match: {
            IsBlock: true,
            Epoch: {$gte: ctx.StartEpoch, $lt: ctx.EndEpoch}
        }
    },
    {
        $lookup: {
            from: "ExecTrace",
            let: {
                id: "$_id",
                epoch: "$Epoch",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                {$eq: ["$Epoch", "$$epoch"]},
                                // {$ne: ["$_id", "$$id"]},
                                {$eq: [{$indexOfBytes: ["$_id", "$$id"]}, 0]},
                            ]
                        }
                    }
                },
            ],
            as: "childTrace",
        }
    },
    {
        $unwind: "$childTrace"
    },
    {
        $lookup: {
            from: "Message",
            let: {
                cid: "$_id",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                {$eq: ["$childTrace.Cid", "$$cid"]},
                                {$or: [ // only v10
                                        {$eq: ["$Detail.Actor", "fil/10/eam"]},
                                        {$eq: ["$Detail.Actor", "fil/10/evm"]},
                                        {$eq: ["$Detail.Actor", "fil/10/ethaccount"]},
                                        {$eq: ["$Detail.Actor", "fil/10/placeholder"]},
                                    ]}
                            ]
                        }
                    }
                },
            ],
            as: "childMessage",
        }
    },
    {
        $unwind: "$childMessage"
    },
    {
        $project: {
            Epoch: "$Epoch",
            Cid: "$Cid"
        }
    }
]