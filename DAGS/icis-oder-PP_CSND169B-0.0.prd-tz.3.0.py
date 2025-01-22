from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSND169B',WORKFLOW_ID='f5043138844145bc842d0b3ebba7cdcc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND169B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f5043138844145bc842d0b3ebba7cdcc')

    csnd169bJob_vol = []
    csnd169bJob_volMnt = []
    csnd169bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd169bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd169bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd169bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnd169bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd169bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd0725d555d51457cb0db5b8999b3049e',
        'volumes': csnd169bJob_vol,
        'volume_mounts': csnd169bJob_volMnt,
        'env_from':csnd169bJob_env,
        'task_id':'csnd169bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csnd169bJob", "endTranDate=20241129", "pgmNm=csnd169b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f5043138844145bc842d0b3ebba7cdcc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd169bJob,
        Complete
    ]) 

    # authCheck >> csnd169bJob >> Complete
    workflow








