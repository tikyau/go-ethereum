import "mortal";

/// @title Chequebook for Ethereum micropayments
/// @author Daniel A. Nagy <daniel@ethdev.com>
contract chequebook is mortal {
    // Cumulative paid amount in wei to each beneficiary
    mapping (address => uint256) sent;

    /// @notice Overdraft event
    event Overdraft(address deadbeat);
    
    /// @notice Accessor for sent map
    ///
    /// @param beneficiary beneficiary address
    /// @return cumulative amount in wei sent to beneficiary
    function getSent(address beneficiary) returns (uint256) {
	    return sent[beneficiary];
    }

    /// @notice Cash cheque
    /// 
    /// @param beneficiary beneficiary address
    /// @param amount cumulative amount in wei
    /// @param sig_v signature parameter v
    /// @param sig_r signature parameter r
    /// @param sig_s signature parameter s
    function cash(address beneficiary, uint256 amount,
        uint8 sig_v, bytes32 sig_r, bytes32 sig_s) {
        // Check if the cheque is old.
        // Only cheques that are more recent than the last cashed one are considered.
        if(amount <= sent[beneficiary]) return;
        // Check the digital signature of the cheque.
        bytes32 hash = sha3(beneficiary, amount);
        if(owner != ecrecover(hash, sig_v, sig_r, sig_s)) return;
        // Attempt sending the difference between the cumulative amount on the cheque
        // and the cumulative amount on the last cashed cheque to beneficiary.
        if (beneficiary.send(amount - sent[beneficiary])) {
            // Upon success, update the cumulative amount.
            sent[beneficiary] = amount;
        } else {
            // Upon failure, punish owner for writing a bounced cheque.
            // owner.sendToDebtorsPrison();
            Overdraft(owner);
            // Compensate beneficiary.
            suicide(beneficiary);
        }
    }
}
