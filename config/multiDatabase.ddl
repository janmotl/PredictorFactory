ALTER TABLE "ctu_crossDatabase1".account
  ADD CONSTRAINT account_ibfk_1 FOREIGN KEY ( district_id ) REFERENCES "ctu_crossDatabase2".district
  ( district_id ) ON DELETE no action;

ALTER TABLE "ctu_crossDatabase2".card
  ADD CONSTRAINT card_ibfk_1 FOREIGN KEY ( disp_id ) REFERENCES "ctu_crossDatabase2".disp ( disp_id )
  ON DELETE no action;

ALTER TABLE "ctu_crossDatabase2".client
  ADD CONSTRAINT client_ibfk_1 FOREIGN KEY ( district_id ) REFERENCES "ctu_crossDatabase2".district (
  district_id ) ON DELETE no action;

ALTER TABLE "ctu_crossDatabase2".disp
  ADD CONSTRAINT disp_ibfk_1 FOREIGN KEY ( account_id ) REFERENCES "ctu_crossDatabase1".account (
  account_id ) ON DELETE no action;

ALTER TABLE "ctu_crossDatabase2".disp
  ADD CONSTRAINT disp_ibfk_2 FOREIGN KEY ( client_id ) REFERENCES "ctu_crossDatabase2".client (
  client_id ) ON DELETE no action;

ALTER TABLE "ctu_crossDatabase1".loan
  ADD CONSTRAINT loan_ibfk_1 FOREIGN KEY ( account_id ) REFERENCES "ctu_crossDatabase1".account (
  account_id ) ON DELETE no action;

ALTER TABLE "ctu_crossDatabase1"."order"
  ADD CONSTRAINT order_ibfk_1 FOREIGN KEY ( account_id ) REFERENCES "ctu_crossDatabase1".account (
  account_id ) ON DELETE no action;

ALTER TABLE "ctu_crossDatabase1".trans
  ADD CONSTRAINT trans_ibfk_1 FOREIGN KEY ( account_id ) REFERENCES "ctu_crossDatabase1".account (
  account_id ) ON DELETE no action;  