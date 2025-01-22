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
                , WORKFLOW_NAME='PP_CSNG187B',WORKFLOW_ID='88f6e6f335e94bce8341fddcb4666497', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG187B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 15, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('88f6e6f335e94bce8341fddcb4666497')

    csng187bJob_vol = []
    csng187bJob_volMnt = []
    csng187bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng187bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng187bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng187bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '659e24bc5a5f4253b3395b90a6741b8f',
        'volumes': csng187bJob_vol,
        'volume_mounts': csng187bJob_volMnt,
        'env_from':csng187bJob_env,
        'task_id':'csng187bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.45',
        'arguments':["--job.name=csng187bJob", "pgmNm=csng187bJob" , "empNo=91350847"
, "endTranDate="+str(datetime.now().strftime("${YYYYMM}"))
, "requestDate="+str(datetime.now().strftime("${YYYYMMDDHHMISSSSS}")) ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('88f6e6f335e94bce8341fddcb4666497')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng187bJob,
        Complete
    ]) 

    # authCheck >> csng187bJob >> Complete
    workflow








