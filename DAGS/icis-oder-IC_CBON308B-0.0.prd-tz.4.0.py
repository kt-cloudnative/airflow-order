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
                , WORKFLOW_NAME='IC_CBON308B',WORKFLOW_ID='068a42a52b354d5c9c1f4a4f7bfdd03f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CBON308B-0.0.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 1, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('068a42a52b354d5c9c1f4a4f7bfdd03f')

    cbon308bJob_vol = []
    cbon308bJob_volMnt = []
    cbon308bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon308bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon308bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbon308bJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    cbon308bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon308bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8d0b691786c548d68dfcf46071222c42',
        'volumes': cbon308bJob_vol,
        'volume_mounts': cbon308bJob_volMnt,
        'env_from':cbon308bJob_env,
        'task_id':'cbon308bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.6',
        'arguments':["--job.name=cbon308bJob", "requestDate=${YYYYMMDDHHMISSSSS}", "endTranDate=20241129", "pgmNm=cbon308b", "ofcCd=710571"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('068a42a52b354d5c9c1f4a4f7bfdd03f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon308bJob,
        Complete
    ]) 

    # authCheck >> cbon308bJob >> Complete
    workflow








